#include <Interpreters/Cache/QueryPlanCache.h>

#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Storages/ColumnDefault.h>


namespace ProfileEvents
{
    extern const Event QueryPlanCacheHits;
    extern const Event QueryPlanCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryPlanCacheBytes;
    extern const Metric QueryPlanCacheEntries;
}

namespace DB
{

bool isSettingIgnoredInQueryPlanCache(std::string_view setting_name)
{
    return setting_name == "allow_experimental_query_plan_cache"
        || setting_name == "enable_query_plan_cache"
        || setting_name == "query_plan_cache_size_in_bytes_quota"
        || setting_name == "log_comment"
        || setting_name == "http_response_headers"
        || setting_name.starts_with("query_cache_")
        || setting_name.ends_with("_query_cache")
        || setting_name.starts_with("output_format_")
        /// Resource limits: do not affect plan shape.
        || setting_name == "max_execution_time"
        || setting_name == "max_memory_usage"
        || setting_name == "max_rows_to_read"
        || setting_name == "max_bytes_to_read"
        || setting_name == "max_result_rows"
        || setting_name == "max_result_bytes"
        || setting_name == "max_rows_to_sort"
        || setting_name == "max_bytes_to_sort"
        || setting_name == "max_rows_in_set"
        || setting_name == "max_bytes_in_set"
        /// Thread control: do not affect plan structure.
        || setting_name == "max_threads"
        || setting_name == "max_insert_threads"
        /// Output: do not affect plan structure.
        || setting_name == "extremes"
        || setting_name == "send_progress_in_http_headers"
        /// Logging / profiling: do not affect plan structure.
        || setting_name.starts_with("log_queries")
        || setting_name == "log_profile_events";
}
bool QueryPlanCacheKey::operator==(const QueryPlanCacheKey & other) const
{
    return ast_hash == other.ast_hash
        && semantic_settings_hash == other.semantic_settings_hash
        && table_metadata_versions == other.table_metadata_versions
        && row_policy_hash == other.row_policy_hash
        && user_id == other.user_id
        && current_user_roles == other.current_user_roles;
}

size_t QueryPlanCacheKeyHasher::operator()(const QueryPlanCacheKey & key) const
{
    SipHash hash;

    /// Hash the 128-bit AST hash as two 64-bit words.
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);

    hash.update(key.semantic_settings_hash);

    /// Hash the per-table metadata versions map.
    for (const auto & [table, version] : key.table_metadata_versions)
    {
        hash.update(table);
        hash.update(version);
    }

    /// Hash row policy expression as two 64-bit words.
    hash.update(key.row_policy_hash.low64);
    hash.update(key.row_policy_hash.high64);

    /// Hash user identity.
    if (key.user_id.has_value())
        hash.update(*key.user_id);

    for (const auto & role_id : key.current_user_roles)
        hash.update(role_id);

    return hash.get64();
}

QueryPlanCache::QueryPlanCache(size_t max_size_in_bytes, size_t max_entries)
    : cache(CurrentMetrics::QueryPlanCacheBytes, CurrentMetrics::QueryPlanCacheEntries, max_size_in_bytes, max_entries)
{
}

void QueryPlanCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries)
{
    cache.setMaxSizeInBytes(max_size_in_bytes);
    cache.setMaxCount(max_entries);
    /// max_count=0 means "unlimited" in LRUCachePolicy, but for query plan cache
    /// we treat 0 as "disabled" -- clear all existing entries.
    if (max_size_in_bytes == 0 || max_entries == 0)
        cache.clear();
}

QueryPlanCache::Cache::MappedPtr QueryPlanCache::get(const QueryPlanCacheKey & key)
{
    auto result = cache.get(key);
    if (result)
    {
        /// Reject entries with incompatible format version.
        if (result->format_version != QUERY_PLAN_CACHE_FORMAT_VERSION)
        {
            cache.remove(key);
            ProfileEvents::increment(ProfileEvents::QueryPlanCacheMisses);
            return nullptr;
        }
        ProfileEvents::increment(ProfileEvents::QueryPlanCacheHits);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::QueryPlanCacheMisses);
    }
    return result;
}

void QueryPlanCache::set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry, size_t max_size_in_bytes_for_user)
{
    /// CacheBase treats max_count=0 as "unlimited", but for query plan cache
    /// we treat 0 as "disabled". Guard against inserting into a disabled cache.
    if (cache.maxSizeInBytes() == 0 || cache.maxCount() == 0)
        return;

    /// The quota is evaluated using the current query context only.
    /// We do not remember any per-user quota inside the shared cache because that would make
    /// one query with quota=0 permanently disable quota checks for the same user.
    if (!canStoreForUser(key, entry, max_size_in_bytes_for_user))
        return;

    cache.set(key, std::make_shared<QueryPlanCacheEntry>(std::move(entry)));
}

void QueryPlanCache::clear()
{
    cache.clear();
}

size_t QueryPlanCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t QueryPlanCache::count() const
{
    return cache.count();
}

std::vector<QueryPlanCache::Cache::KeyMapped> QueryPlanCache::dump() const
{
    return cache.dump();
}

bool QueryPlanCache::canStoreForUser(const QueryPlanCacheKey & key, const QueryPlanCacheEntry & entry, size_t max_size_in_bytes_for_user) const
{
    if (!key.user_id.has_value() || max_size_in_bytes_for_user == 0)
        return true;

    /// This is an admission check, not durable accounting.
    /// Existing entries inserted by previous queries remain valid even if the current query
    /// now uses a smaller quota.
    const size_t new_entry_size = QueryPlanCacheEntryWeight{}(entry);

    /// Early rejection: a single entry larger than the quota can never fit.
    if (new_entry_size > max_size_in_bytes_for_user)
        return false;

    size_t current_size_for_user = 0;
    for (const auto & [existing_key, existing_entry] : cache.dump())
    {
        if (existing_key.user_id != key.user_id)
            continue;

        if (existing_key == key)
            continue;

        current_size_for_user += QueryPlanCacheEntryWeight{}(*existing_entry);

        /// Early break: once we know the quota is exceeded, no need to keep summing.
        if (current_size_for_user + new_entry_size > max_size_in_bytes_for_user)
            return false;
    }

    return current_size_for_user + new_entry_size <= max_size_in_bytes_for_user;
}

UInt64 SemanticSettings::computeHash(const Settings & settings)
{
    SipHash hash;

    for (const auto setting_name : settings.getAllRegisteredNames())
    {
        if (isSettingIgnoredInQueryPlanCache(setting_name))
            continue;

        hash.update(setting_name);
        applyVisitor(FieldVisitorHash(hash), settings.get(setting_name));
    }

    return hash.get64();
}

Int64 computeSchemaHash(const StorageInMemoryMetadata & metadata)
{
    SipHash hash;

    /// Hash all columns: name + type + default kind + default expression.
    /// The default kind (Default/Materialized/Alias/Ephemeral) and its expression
    /// must be part of the hash so that ALTER TABLE MODIFY COLUMN ... ALIAS/DEFAULT
    /// invalidates cached plans. ALIAS columns are evaluated at SELECT time and
    /// directly affect query results; DEFAULT/MATERIALIZED changes can also alter
    /// implicit column references in cached plans.
    for (const auto & column : metadata.columns)
    {
        hash.update(column.name);
        hash.update(column.type->getName());
        hash.update(static_cast<UInt8>(column.default_desc.kind));
        if (column.default_desc.expression)
            hash.update(column.default_desc.expression->formatForLogging());
    }

    /// Hash sorting key expression
    if (metadata.sorting_key.expression_list_ast)
        hash.update(metadata.sorting_key.expression_list_ast->formatForLogging());

    /// Hash partition key expression
    if (metadata.partition_key.expression_list_ast)
        hash.update(metadata.partition_key.expression_list_ast->formatForLogging());

    /// Hash primary key expression (may differ from sorting key)
    if (metadata.primary_key.expression_list_ast)
        hash.update(metadata.primary_key.expression_list_ast->formatForLogging());

    /// Mask to positive Int64 range to distinguish from metadata_version (small positive integers)
    return static_cast<Int64>(hash.get64() & 0x7FFFFFFFFFFFFFFF);
}

}
