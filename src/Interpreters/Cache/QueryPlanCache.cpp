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
    /// Cache control: settings that select or sidestep the cache itself.
    return setting_name == "allow_experimental_query_plan_cache"
        || setting_name == "enable_query_plan_cache"
        || setting_name == "query_plan_cache_size_in_bytes_quota"
        || setting_name.starts_with("query_cache_")
        || setting_name.ends_with("_query_cache")
        /// Output formatting: applied after the plan and never baked into a plan step.
        || setting_name.starts_with("output_format_")
        || setting_name == "send_progress_in_http_headers"
        || setting_name == "http_response_headers"
        /// Generic resource limits: enforced by execution-time guards, not stored on any plan step.
        || setting_name == "max_execution_time"
        || setting_name == "max_memory_usage"
        || setting_name == "max_rows_to_read"
        || setting_name == "max_bytes_to_read"
        || setting_name == "max_result_rows"
        || setting_name == "max_result_bytes"
        || setting_name == "max_rows_in_set"
        || setting_name == "max_bytes_in_set"
        /// Thread fan-out is decided when the pipeline is built from the plan, not stored
        /// on the plan itself, so different thread counts can safely share a cached plan.
        || setting_name == "max_threads"
        || setting_name == "max_insert_threads"
        /// Logging and profiling: orthogonal to plan structure.
        || setting_name == "log_comment"
        || setting_name.starts_with("log_queries")
        || setting_name == "log_profile_events";
    /// Sort-related limits (`max_rows_to_sort`, `max_bytes_to_sort`) and `extremes` are
    /// intentionally NOT ignored: they are baked into `SortingStep` / `ExtremesStep`,
    /// so two queries with different values of these settings must not share a plan.
}
bool QueryPlanCacheKey::operator==(const QueryPlanCacheKey & other) const
{
    return ast_hash == other.ast_hash
        && current_database == other.current_database
        && semantic_settings_hash == other.semantic_settings_hash
        && table_metadata_versions == other.table_metadata_versions
        && storage_id.uuid == other.storage_id.uuid
        && row_policy_hash == other.row_policy_hash
        && user_id == other.user_id
        && current_user_roles == other.current_user_roles;
}

bool QueryPlanCacheDependencyFingerprint::operator==(const QueryPlanCacheDependencyFingerprint & other) const
{
    return storage_id.uuid == other.storage_id.uuid
        && table_metadata_versions == other.table_metadata_versions
        && row_policy_hash == other.row_policy_hash
        && row_policy_names_hash == other.row_policy_names_hash
        && semantic_settings_hash == other.semantic_settings_hash
        && selected_columns == other.selected_columns;
}

size_t QueryPlanCacheKeyHasher::operator()(const QueryPlanCacheKey & key) const
{
    SipHash hash;

    /// Hash the 128-bit AST hash as two 64-bit words.
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);
    hash.update(key.current_database);

    hash.update(key.semantic_settings_hash);

    /// Hash the per-table metadata versions map.
    for (const auto & [table, version] : key.table_metadata_versions)
    {
        hash.update(table);
        hash.update(version);
    }

    /// Hash storage UUID so that DROP+CREATE of the same name in an Atomic database
    /// does not collide with cached entries from the previous table.
    hash.update(key.storage_id.uuid);

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
    : Base(CurrentMetrics::QueryPlanCacheBytes, CurrentMetrics::QueryPlanCacheEntries, max_size_in_bytes, max_entries)
{
}

void QueryPlanCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries)
{
    Base::updateConfiguration(max_size_in_bytes, max_entries);
    /// max_count=0 means "unlimited" in LRUCachePolicy, but for query plan cache
    /// we treat 0 as "disabled" -- clear all existing entries.
    if (max_size_in_bytes == 0 || max_entries == 0)
        Base::clear();
}

QueryPlanCache::MappedPtr QueryPlanCache::get(const QueryPlanCacheKey & key)
{
    auto result = Base::get(key);
    if (result)
    {
        /// Reject entries with incompatible format version.
        if (result->format_version != QUERY_PLAN_CACHE_FORMAT_VERSION)
        {
            Base::removeIfMatches(key, result);
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
    Base::setOwned(
        key,
        std::make_shared<QueryPlanCacheEntry>(std::move(entry)),
        Base::OwnerQuota{key.user_id, max_size_in_bytes_for_user},
        /*zero_max_count_is_disabled=*/true);
}

void QueryPlanCache::clear()
{
    Base::clear();
}

size_t QueryPlanCache::sizeInBytes() const
{
    return Base::sizeInBytes();
}

size_t QueryPlanCache::count() const
{
    return Base::count();
}

std::vector<QueryPlanCache::KeyMapped> QueryPlanCache::dump() const
{
    return Base::dump();
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
