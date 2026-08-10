#pragma once

#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IASTHash.h>
#include <Storages/ColumnDefault.h>
#include <Common/CacheBase.h>
#include <Common/SipHash.h>

#include <mutex>
#include <string>
#include <string_view>
#include <vector>

namespace DB
{

struct Settings;

/// Identifies a cached query plan. Two queries with the same key can safely share a cached plan.
struct QueryPlanCacheKey
{
    /// 128-bit hash of `ast_identity`, used only by the hasher.
    IASTHash ast_hash;

    /// Exact one-line representation of the normalized AST. It contains secrets and must never
    /// be exposed through logs or system tables.
    String ast_identity;

    /// Session current database used to resolve unqualified identifiers, including table names
    /// in `additional_table_filters`.
    String current_database;

    /// SipHash of plan-affecting settings only (see SemanticSettings).
    UInt64 semantic_settings_hash = 0;

    bool operator==(const QueryPlanCacheKey & other) const;
};

/// Information collected before query analysis. It is safe to use for cache lookup,
/// but a found entry still needs dependency validation before execution.
struct QueryPlanCacheLookupContext
{
    QueryPlanCacheKey key;
    StorageID storage_id = StorageID::createEmpty();
};

/// The part of a column definition that can affect a cached read plan. Comments, codecs,
/// statistics and TTLs are intentionally excluded because they do not change read semantics.
struct QueryPlanCacheColumnDependency
{
    String name;
    String type;
    ColumnDefaultKind default_kind = ColumnDefaultKind::Default;
    String default_expression;
    bool ephemeral_default = false;

    bool operator==(const QueryPlanCacheColumnDependency & other) const = default;
};

/// Storage contract captured from every universalized `ReadFromTableStep`.
struct QueryPlanCacheStorageDependency
{
    String table_name;
    String engine_name;
    std::vector<QueryPlanCacheColumnDependency> columns;
    String sorting_key;
    String primary_key;
    std::vector<bool> sorting_key_reverse_flags;

    bool operator==(const QueryPlanCacheStorageDependency & other) const = default;
};

/// A serialized query plan stored in the cache.
struct QueryPlanCacheEntry
{
    /// Binary-serialized `QueryPlan` bytes.
    std::string serialized_plan;

    /// Columns selected by query semantics. This can be empty for queries such as `SELECT count()`.
    Names selected_columns;

    /// Physical columns read by the cached plan. They are used for dependency validation and
    /// query-access logging, but not as an additional privilege requirement.
    Names read_columns;

    /// Planner/storage dependencies captured from universalized read leaves.
    std::vector<QueryPlanCacheStorageDependency> dependencies;

    /// Approximate key bytes charged to the cache entry weight.
    size_t key_size_in_bytes = 0;
};

/// Hasher for `QueryPlanCacheKey`. The exact AST identity is checked by `operator==`.
struct QueryPlanCacheKeyHasher
{
    size_t operator()(const QueryPlanCacheKey & key) const;
};

struct QueryPlanCacheEntryWeight
{
    size_t operator()(const QueryPlanCacheEntry & entry) const
    {
        size_t weight = entry.serialized_plan.size() + entry.key_size_in_bytes;
        for (const auto & column : entry.selected_columns)
            weight += column.size();
        for (const auto & column : entry.read_columns)
            weight += column.size();
        for (const auto & dependency : entry.dependencies)
        {
            weight += dependency.table_name.size();
            weight += dependency.engine_name.size();
            weight += dependency.sorting_key.size();
            weight += dependency.primary_key.size();
            weight += dependency.sorting_key_reverse_flags.size();
            for (const auto & column : dependency.columns)
            {
                weight += column.name.size();
                weight += column.type.size();
                weight += column.default_expression.size();
            }
        }
        return weight;
    }
};

/// Thread-safe LRU/SLRU cache mapping `QueryPlanCacheKey` to `QueryPlanCacheEntry`.
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

    MappedPtr get(const QueryPlanCacheKey & key);
    void set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry);

    void clear();
    size_t sizeInBytes() const;
    size_t count() const;
    std::vector<KeyMapped> dump() const;

private:
    mutable std::mutex configuration_mutex;
    size_t configured_max_size TSA_GUARDED_BY(configuration_mutex);
    size_t configured_max_entries TSA_GUARDED_BY(configuration_mutex);
};

using QueryPlanCachePtr = std::shared_ptr<QueryPlanCache>;

struct SemanticSettings
{
    static UInt64 computeHash(const Settings & settings);
};

bool isSettingIgnoredInQueryPlanCache(std::string_view setting_name);

}
