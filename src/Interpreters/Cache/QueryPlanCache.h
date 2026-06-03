#pragma once

#include <Common/CacheBase.h>
#include <Common/SipHash.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IASTHash.h>
#include <Interpreters/StorageID.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/UUID.h>

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace DB
{

struct Settings;

/// Format version for cache entries. Must be incremented when the serialization format changes.
inline constexpr UInt64 QUERY_PLAN_CACHE_FORMAT_VERSION = 2;

/// Identifies a cached query plan. Two queries with the same key can safely share a cached plan.
/// The key includes:
///   - AST hash: captures the query shape independent of whitespace and case differences
///   - Semantic settings hash: only settings that affect the query plan (not resource limits)
///   - Per-table metadata versions: invalidates cache on schema changes (ALTER TABLE)
///   - User identity: user_id + role IDs, prevents cross-user cache sharing (row policy isolation)
struct QueryPlanCacheKey
{
    /// 128-bit CityHash of the query AST (same type used by QueryResultCache)
    IASTHash ast_hash;

    /// SipHash of plan-affecting settings only (see SemanticSettings)
    UInt64 semantic_settings_hash = 0;

    /// Per-table schema version: {database.table -> metadata_version}
    /// Incremented by ReplicatedMergeTree on schema changes; for non-replicated tables,
    /// we use a schema content hash as a stable proxy.
    /// TODO: For View support, this map must also track the View definition version itself
    /// (not just the underlying table versions). A change in the View's SQL definition must
    /// invalidate cached plans even if the underlying table schema stays the same.
    std::map<String, Int64> table_metadata_versions;

    /// StorageID of the queried table; used for access rights revalidation on cache hit.
    /// Only its `uuid` (when present) participates in hash/equality — this distinguishes
    /// `DROP TABLE` + `CREATE TABLE` of the same name in an Atomic database from the
    /// original table, even when their schemas are identical.
    StorageID storage_id = StorageID::createEmpty();

    /// Hash of the applicable row policy expression AST. Different row policies produce
    /// different filtered views of the data, so cached plans must not be shared across them.
    IASTHash row_policy_hash{};

    /// User identity for row policy isolation.
    /// INVARIANT: current_user_roles must be sorted before constructing the key.
    /// Vector equality is order-sensitive; non-deterministic ordering would produce different
    /// keys for semantically identical role sets.
    std::optional<UUID> user_id;
    std::vector<UUID> current_user_roles;

    bool operator==(const QueryPlanCacheKey & other) const;
};

/// A serialized query plan stored in the cache.
struct QueryPlanCacheEntry
{
    /// Must match QUERY_PLAN_CACHE_FORMAT_VERSION. Entries with a different version are rejected.
    UInt64 format_version = QUERY_PLAN_CACHE_FORMAT_VERSION;

    /// Binary-serialized QueryPlan bytes (produced by QueryPlan::ensureSerialized / getSerializedData)
    std::string serialized_plan;

    /// Columns that require `SELECT` access revalidation on cache hit.
    /// Empty means "any accessible column is sufficient", matching `SELECT count()` semantics.
    Names selected_columns;

    /// Row policy names applied when the plan was originally built.
    /// Persisted so that cache-hit paths can propagate them to system.query_log.
    std::set<String> used_row_policies;

    /// User who inserted this entry. Stamped by `QueryPlanCache::set` so that the
    /// eviction callback can decrement the right per-user accounting bucket.
    /// Not part of the cache key and never serialized.
    std::optional<UUID> inserter_user_id;

    /// Cache key under which this entry is stored. Stamped by `QueryPlanCache::set`
    /// so that `onEntryRemoval` can erase the matching `entry_weights` record on
    /// LRU/SLRU eviction. Not part of the cache key and never serialized.
    std::optional<QueryPlanCacheKey> cache_key;
};

/// Hasher for QueryPlanCacheKey. Uses SipHash over all identifying fields.
struct QueryPlanCacheKeyHasher
{
    size_t operator()(const QueryPlanCacheKey & key) const;
};

/// Weight function for QueryPlanCacheEntry: returns the byte size of the serialized plan
/// plus the byte size of selected column names.
struct QueryPlanCacheEntryWeight
{
    size_t operator()(const QueryPlanCacheEntry & entry) const
    {
        size_t weight = entry.serialized_plan.size();
        for (const auto & col : entry.selected_columns)
            weight += col.size();
        for (const auto & policy : entry.used_row_policies)
            weight += policy.size();
        return weight;
    }
};

/// Thread-safe LRU/SLRU cache mapping QueryPlanCacheKey -> QueryPlanCacheEntry.
/// Follows the same design pattern as QueryResultCache (both use CacheBase).
class QueryPlanCache : private CacheBase<QueryPlanCacheKey, QueryPlanCacheEntry, QueryPlanCacheKeyHasher, QueryPlanCacheEntryWeight>
{
private:
    using Base = CacheBase<QueryPlanCacheKey, QueryPlanCacheEntry, QueryPlanCacheKeyHasher, QueryPlanCacheEntryWeight>;

public:
    using Cache = Base;
    using typename Base::KeyMapped;
    using typename Base::MappedPtr;

    QueryPlanCache(size_t max_size_in_bytes, size_t max_entries);

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries);

    /// Looks up an entry. Returns nullptr on miss or version mismatch.
    MappedPtr get(const QueryPlanCacheKey & key);

    /// Stores an entry. Takes ownership by value to allow move-construction and avoid copying
    /// the serialized_plan string (which may be several kilobytes).
    ///
    /// `max_size_in_bytes_for_user` is interpreted only for this insertion attempt.
    /// It comes from the current query's `query_plan_cache_size_in_bytes_quota` setting
    /// and is not persisted in the cache. `0` means unlimited.
    void set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry, size_t max_size_in_bytes_for_user = 0);

    void clear();

    size_t sizeInBytes() const;
    size_t count() const;

    /// Exposes the underlying cache for system table introspection
    std::vector<KeyMapped> dump() const;

protected:
    /// Called by CacheBase whenever an entry is removed (replacement or LRU eviction).
    /// Used to keep `per_user_bytes` in sync without scanning the whole cache.
    void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr) override;

private:
    /// Best-effort admission check against the current query's quota.
    /// O(1): reads the cached per-user byte count rather than scanning the cache.
    bool canStoreForUser(const QueryPlanCacheKey & key, const QueryPlanCacheEntry & entry, size_t max_size_in_bytes_for_user) const;

    mutable std::mutex per_user_mutex;
    std::unordered_map<UUID, size_t> per_user_bytes TSA_GUARDED_BY(per_user_mutex);

    /// Per-key weight tracker. `LRUCachePolicy::set` and `SLRUCachePolicy::set` overwrite
    /// existing cells silently (without invoking `onEntryRemoval`), so `set` cannot rely on
    /// the eviction hook to compensate the previous weight on a same-key replacement.
    /// We track the inserter+weight here at insert time, decrement on replacement, and
    /// erase on eviction inside `onEntryRemoval`.
    std::unordered_map<QueryPlanCacheKey, std::pair<std::optional<UUID>, size_t>, QueryPlanCacheKeyHasher>
        entry_weights TSA_GUARDED_BY(per_user_mutex);
};

using QueryPlanCachePtr = std::shared_ptr<QueryPlanCache>;


/// Computes a hash over all builtin settings except those explicitly known not to affect
/// query planning. This is intentionally conservative: cache misses are cheaper than
/// reusing a plan built under incompatible planner settings.
struct SemanticSettings
{
    /// Computes a 64-bit hash over builtin settings from the provided Settings object,
    /// excluding settings explicitly ignored by the query plan cache.
    static UInt64 computeHash(const Settings & settings);
};

/// Returns true for settings that must not affect the query plan cache key.
bool isSettingIgnoredInQueryPlanCache(std::string_view setting_name);

/// Computes a stable hash over table schema for non-replicated tables whose
/// `metadata_version` is always 0. Covers physical columns (names + types),
/// sorting key, and partition key.
Int64 computeSchemaHash(const StorageInMemoryMetadata & metadata);

}
