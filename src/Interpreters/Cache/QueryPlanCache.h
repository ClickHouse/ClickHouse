#pragma once

#include <Common/CacheBase.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IASTHash.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <base/UUID.h>

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
inline constexpr UInt64 QUERY_PLAN_CACHE_FORMAT_VERSION = 1;

/// Identifies a cached query plan. Two queries with the same key can safely share a cached plan
/// once the entry dependencies are revalidated.
///
/// Unlike a per-table design, the key deliberately contains no storage information: for queries
/// over views, joins and subqueries the set of referenced tables is only known after analysis,
/// which is exactly the work the cache is meant to skip. All storage-related state lives in the
/// entry's dependency fingerprint and is revalidated on every hit.
struct QueryPlanCacheKey
{
    /// 128-bit CityHash of the normalized query AST (same type used by QueryResultCache).
    IASTHash ast_hash;

    /// SipHash of plan-affecting settings only (see SemanticSettings).
    UInt64 semantic_settings_hash = 0;

    /// Unqualified table names resolve against the current database, so the same query text
    /// run from different databases must not share a plan.
    String current_database;

    /// User identity: a plan bakes row policies and was admitted under this user's access
    /// rights, so it must not be shared across users or differing role sets.
    /// INVARIANT: current_user_roles must be sorted before constructing the key.
    std::optional<UUID> user_id;
    std::vector<UUID> current_user_roles;

    bool operator==(const QueryPlanCacheKey & other) const;
};

/// One referenced storage (base table or view) of a cached plan.
/// Revalidated on every cache hit before the plan is executed.
struct QueryPlanCacheDependency
{
    String database;
    String table;

    /// UUID at plan time: detects DROP + CREATE of the same name in an Atomic database.
    UUID uuid = UUIDHelpers::Nil;

    /// Metadata version (ReplicatedMergeTree) or a schema content hash for storages whose
    /// metadata_version stays 0. For views the hash also covers the view's SELECT query,
    /// because the view body is baked into the cached plan.
    Int64 metadata_version = 0;

    /// Hash of the row policy filter applicable to this user on this table. Policies are baked
    /// into the cached plan as filter steps, so a policy change must invalidate the entry.
    IASTHash row_policy_hash{};

    /// Columns the plan reads from this storage, i.e. the physical columns of the leaf read.
    /// Reported to `system.query_log` on a hit, mirroring what a miss records.
    /// Empty means no specific columns were read at this leaf (e.g. a `count()` over the table).
    Names columns;

    /// Columns whose `SELECT` privilege the planner verified for this storage; rechecked for
    /// SELECT access on every hit. This is the *logical* set the query selects, `ALIAS` columns
    /// included, which differs from `columns`: `SELECT b FROM t` with `b ALIAS a + 1` checks
    /// `SELECT(b)` while reading `a`. Rechecking the physical set instead would let a hit serve
    /// `b` after `REVOKE SELECT(b)`, and falsely deny a user who only holds `SELECT(b)`.
    /// Empty (with `columns_unknown` false) means no specific column was required, in which case
    /// the recheck applies the same "SELECT on at least one column" rule as planning.
    Names access_checked_columns;

    /// True when the exact set of columns read from this storage is unknown because the storage
    /// was discovered through the AST closure rather than a plan leaf: a view body, or a table
    /// referenced only from a scalar subquery folded into a constant. Such a dependency requires
    /// table-level `SELECT` on a hit, because a column-level recheck could pass while the baked
    /// plan still reads a column whose grant was revoked after the entry was stored.
    bool columns_unknown = false;

    /// True for view dependencies discovered through the AST closure (not read at runtime).
    bool is_view = false;

    bool operator==(const QueryPlanCacheDependency & other) const;
};

/// A serialized query plan stored in the cache.
struct QueryPlanCacheEntry
{
    /// Must match QUERY_PLAN_CACHE_FORMAT_VERSION. Entries with a different version are rejected.
    UInt64 format_version = QUERY_PLAN_CACHE_FORMAT_VERSION;

    /// Binary-serialized logical QueryPlan (leaf reads are storage-agnostic `ReadFromTable`
    /// steps, produced by the planner's `build_logical_plan` mode).
    std::string serialized_plan;

    /// All storages the plan depends on, in a deterministic order.
    std::vector<QueryPlanCacheDependency> dependencies;

    /// Row policy names applied when the plan was originally built.
    /// Persisted so that cache-hit paths can propagate them to `system.query_log`.
    std::set<String> used_row_policies;

    /// User who inserted this entry. Stamped by `QueryPlanCache::set` so that the
    /// eviction callback can decrement the right per-user accounting bucket.
    std::optional<UUID> inserter_user_id;

    /// Cache key under which this entry is stored. Stamped by `QueryPlanCache::set`
    /// so that `onEntryRemoval` can erase the matching `entry_weights` record on
    /// LRU/SLRU eviction.
    std::optional<QueryPlanCacheKey> cache_key;
};

struct QueryPlanCacheKeyHasher
{
    size_t operator()(const QueryPlanCacheKey & key) const;
};

struct QueryPlanCacheEntryWeight
{
    size_t operator()(const QueryPlanCacheEntry & entry) const
    {
        size_t weight = entry.serialized_plan.size();
        for (const auto & dep : entry.dependencies)
        {
            weight += dep.database.size() + dep.table.size() + sizeof(dep.uuid) + sizeof(dep.metadata_version)
                + sizeof(dep.row_policy_hash) + sizeof(dep.columns_unknown) + sizeof(dep.is_view);
            for (const auto & col : dep.columns)
                weight += col.size();
            /// `access_checked_columns` is a second, usually similarly sized, `Names` vector held by
            /// every dependency, so leaving it out systematically undercounts complex entries.
            for (const auto & col : dep.access_checked_columns)
                weight += col.size();
        }
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

    /// True when the server configuration allows the cache to hold entries. The cache object is
    /// created unconditionally at startup, so callers must ask this rather than rely on the object
    /// existing. Mirrors the "disabled" condition enforced in `set` and `updateConfiguration`:
    /// `CacheBase` treats a zero limit as "unlimited", the query plan cache treats it as "off".
    bool isEnabled() const { return maxSizeInBytes() > 0 && maxCount() > 0; }

    /// Looks up an entry. Returns nullptr on miss or version mismatch.
    MappedPtr get(const QueryPlanCacheKey & key);

    /// Stores an entry. `max_size_in_bytes_for_user` comes from the current query's
    /// `query_plan_cache_size_in_bytes_quota` setting; 0 means no quota.
    /// Returns whether the entry was admitted. Admission fails when the cache is disabled, the
    /// user's quota has no room for the entry, or the entry is larger than the whole cache - all
    /// states that persist across identical executions, so the caller must not keep executing the
    /// cacheable logical plan (which has ordinary planner behaviors switched off) for a query that
    /// can never produce a hit.
    bool set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry, size_t max_size_in_bytes_for_user = 0);

    /// Removes the entry stored under `key`, but only while it is still `entry`. Called for an
    /// entry that failed validation or materialization: such an entry must be evicted rather than
    /// merely skipped, because when the query became permanently uncacheable (e.g. a view switched
    /// away from `INVOKER` security) nothing ever replaces it, and a dead plan would stay resident
    /// forever, pinning cache size and per-user quota. A fresh plan stored concurrently under the
    /// same key is left untouched.
    void remove(const QueryPlanCacheKey & key, const MappedPtr & entry);

    void clear();

    size_t sizeInBytes() const;
    size_t count() const;

    std::vector<KeyMapped> dump() const;

protected:
    /// Called by CacheBase whenever an entry is removed (replacement or LRU eviction).
    /// Used to keep `per_user_bytes` in sync without scanning the whole cache.
    void onEntryRemoval(size_t weight_loss, const MappedPtr & mapped_ptr) override;

private:
    /// Best-effort admission check against the current query's quota.
    /// O(1): reads the cached per-user byte count rather than scanning the cache.
    bool canStoreForUser(const QueryPlanCacheKey & key, const QueryPlanCacheEntry & entry, size_t max_size_in_bytes_for_user) const;

    /// Subtracts `weight` from a user's tracked bytes, erasing the record when it reaches zero.
    /// The caller must hold `per_user_mutex`.
    void decrementUserBytes(const std::optional<UUID> & user_id, size_t weight) TSA_REQUIRES(per_user_mutex);

    /// Serializes `set` against `clear`, so that admission accounting and residency always agree.
    /// `set` charges `per_user_bytes`/`entry_weights` before `Base::set` (the entry can be evicted
    /// synchronously inside that call, and the eviction hook must find the charge already present),
    /// while `clear` drops residency in `Base::clear` before wiping the accounting maps. Without an
    /// outer lock, a `clear` landing between a `set`'s charge and its insertion would wipe the
    /// accounting of an entry that then becomes resident, leaving it unaccounted and undercounting
    /// `query_plan_cache_size_in_bytes_quota` until that key is inserted again.
    ///
    /// It has to be a separate mutex rather than `per_user_mutex`: holding `per_user_mutex` across
    /// `Base::set` would invert the lock order, because synchronous eviction inside `Base::set`
    /// calls `onEntryRemoval` (which takes `per_user_mutex`) while `CacheBase` holds its own mutex.
    /// Nothing acquires `admission_mutex` while holding `CacheBase`'s mutex or `per_user_mutex`, so
    /// the order stays `admission_mutex` -> `CacheBase::mutex` -> `per_user_mutex`.
    std::mutex admission_mutex;

    mutable std::mutex per_user_mutex;
    std::unordered_map<UUID, size_t> per_user_bytes TSA_GUARDED_BY(per_user_mutex);

    /// Accounting for one entry, tracked per key by `entry_weights`.
    struct EntryWeight
    {
        std::optional<UUID> user_id;
        size_t weight = 0;

        /// Identity of the entry this record accounts for, compared (never dereferenced) so that
        /// `remove` releases the charge only while `entry_weights[key]` still refers to the exact
        /// entry it evicted. Between `remove`'s residency check and its accounting a concurrent
        /// same-key `set` may have replaced both the resident plan and this record; that fresh
        /// plan's charge must survive. `set` holds the entry alive for the whole call, so the
        /// address can never be reused underneath a live comparison.
        const QueryPlanCacheEntry * entry = nullptr;
    };

    /// Per-key weight tracker. `LRUCachePolicy::set` and `SLRUCachePolicy::set` overwrite
    /// existing cells silently (without invoking `onEntryRemoval`), so `set` cannot rely on
    /// the eviction hook to compensate the previous weight on a same-key replacement.
    std::unordered_map<QueryPlanCacheKey, EntryWeight, QueryPlanCacheKeyHasher>
        entry_weights TSA_GUARDED_BY(per_user_mutex);
};

using QueryPlanCachePtr = std::shared_ptr<QueryPlanCache>;

/// Computes a hash over all builtin settings except those explicitly known not to affect
/// query planning. This is intentionally conservative: cache misses are cheaper than
/// reusing a plan built under incompatible planner settings.
struct SemanticSettings
{
    static UInt64 computeHash(const Settings & settings);
};

/// Returns true for settings that must not affect the query plan cache key.
bool isSettingIgnoredInQueryPlanCache(std::string_view setting_name);

/// Computes a stable hash over table schema for storages whose `metadata_version` is always 0.
/// Covers physical columns (names + types + defaults), sorting/partition/primary keys, and -
/// crucially for views - the stored SELECT query.
Int64 computeSchemaHash(const StorageInMemoryMetadata & metadata);

}
