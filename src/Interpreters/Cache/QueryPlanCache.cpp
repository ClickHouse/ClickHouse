#include <Interpreters/Cache/QueryPlanCache.h>

#include <Core/Settings.h>
#include <Common/CurrentMetrics.h>
#include <Common/FieldVisitorHash.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

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
    return setting_name == "allow_experimental_query_plan_cache" || setting_name == "enable_query_plan_cache"
        || setting_name.starts_with("query_cache_")
        || setting_name.ends_with("_query_cache")
        /// Output formatting: applied after the plan and never baked into a plan step.
        || setting_name.starts_with("output_format_") || setting_name == "send_progress_in_http_headers"
        || setting_name == "http_response_headers"
        /// Generic resource limits: enforced by execution-time guards, not stored on any plan step.
        || setting_name == "max_execution_time" || setting_name == "max_memory_usage" || setting_name == "max_rows_to_read"
        || setting_name == "max_bytes_to_read" || setting_name == "max_result_rows" || setting_name == "max_result_bytes"
        || setting_name == "max_rows_in_set"
        || setting_name == "max_bytes_in_set"
        /// Thread fan-out is refreshed while materializing or building the pipeline.
        || setting_name == "max_threads"
        || setting_name == "max_insert_threads"
        /// Logging and profiling: orthogonal to plan structure.
        || setting_name == "log_comment" || setting_name.starts_with("log_queries") || setting_name == "log_profile_events";
    /// Sort-related limits (`max_rows_to_sort`, `max_bytes_to_sort`) and `extremes` are
    /// intentionally not ignored because they are baked into plan steps.
}

bool QueryPlanCacheKey::operator==(const QueryPlanCacheKey & other) const
{
    return ast_hash == other.ast_hash && ast_identity == other.ast_identity && current_database == other.current_database
        && semantic_settings_hash == other.semantic_settings_hash;
}

size_t QueryPlanCacheKeyHasher::operator()(const QueryPlanCacheKey & key) const
{
    SipHash hash;
    hash.update(key.ast_hash.low64);
    hash.update(key.ast_hash.high64);
    hash.update(key.current_database);
    hash.update(key.semantic_settings_hash);
    return hash.get64();
}

QueryPlanCache::QueryPlanCache(size_t max_size_in_bytes, size_t max_entries)
    : Base(CurrentMetrics::QueryPlanCacheBytes, CurrentMetrics::QueryPlanCacheEntries, max_size_in_bytes, max_entries)
    , configured_max_size(max_size_in_bytes)
    , configured_max_entries(max_entries)
{
}

void QueryPlanCache::updateConfiguration(size_t max_size_in_bytes, size_t max_entries)
{
    std::lock_guard lock(configuration_mutex);
    configured_max_size = max_size_in_bytes;
    configured_max_entries = max_entries;
    Base::setMaxSizeInBytes(max_size_in_bytes);
    Base::setMaxCount(max_entries);

    /// `max_count = 0` means unlimited in the generic cache, but zero disables this cache.
    if (max_size_in_bytes == 0 || max_entries == 0)
        Base::clear();
}

QueryPlanCache::MappedPtr QueryPlanCache::get(const QueryPlanCacheKey & key)
{
    auto result = Base::get(key);
    ProfileEvents::increment(result ? ProfileEvents::QueryPlanCacheHits : ProfileEvents::QueryPlanCacheMisses);
    return result;
}

void QueryPlanCache::set(const QueryPlanCacheKey & key, QueryPlanCacheEntry entry)
{
    std::lock_guard lock(configuration_mutex);
    if (configured_max_size == 0 || configured_max_entries == 0)
        return;

    entry.key_size_in_bytes = key.ast_identity.size() + key.current_database.size();
    Base::set(key, std::make_shared<QueryPlanCacheEntry>(std::move(entry)));
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

}
