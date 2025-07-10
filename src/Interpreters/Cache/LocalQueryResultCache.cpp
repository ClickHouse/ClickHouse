#include <Interpreters/Cache/LocalQueryResultCache.h>
#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Parsers/TokenIterator.h>
#include <Columns/IColumn.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Common/formatReadable.h>
#include <Common/quoteString.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event QueryCacheHits;
    extern const Event QueryCacheMisses;
}

namespace CurrentMetrics
{
    extern const Metric QueryCacheBytes;
    extern const Metric QueryCacheEntries;
}

namespace DB
{
namespace Setting
{
    extern const SettingsString query_cache_tag;
}

bool LocalQueryResultCache::IsStale::operator()(const Key & key) const
{
    return (key.expires_at < std::chrono::system_clock::now());
};

LocalQueryResultCache::LocalQueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : cache(std::make_unique<TTLCachePolicy<Key, Entry, KeyHasher, QueryResultCacheEntryWeight, IsStale>>(
            CurrentMetrics::QueryCacheBytes, CurrentMetrics::QueryCacheEntries, std::make_unique<PerUserTTLCachePolicyUserQuota>()))
{
    updateConfigurationImpl(max_size_in_bytes, max_entries, max_entry_size_in_bytes_, max_entry_size_in_rows_);
}

void LocalQueryResultCache::updateConfigurationImpl(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache.setMaxSizeInBytes(max_size_in_bytes);
    cache.setMaxCount(max_entries);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
}

void LocalQueryResultCache::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    size_t query_result_cache_max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
    size_t query_result_cache_max_entries = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
    size_t query_result_cache_max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
    size_t query_result_cache_max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
    updateConfigurationImpl(query_result_cache_max_size_in_bytes, query_result_cache_max_entries, query_result_cache_max_entry_size_in_bytes, query_result_cache_max_entry_size_in_rows);
}

std::shared_ptr<QueryResultCacheReader> LocalQueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return std::make_shared<LocalQueryResultCacheReader>(cache, key, lock);
}

std::shared_ptr<QueryResultCacheWriter> LocalQueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t max_query_result_cache_size_in_bytes_quota,
    size_t max_query_result_cache_entries_quota)
{
    /// Update the per-user cache quotas with the values stored in the query context. This happens per query which writes into the query
    /// cache. Obviously, this is overkill but I could find the good place to hook into which is called when the settings profiles in
    /// users.xml change.
    /// user_id == std::nullopt is the internal user for which no quota can be configured
    if (key.user_id.has_value())
        cache.setQuotaForUser(*key.user_id, max_query_result_cache_size_in_bytes_quota, max_query_result_cache_entries_quota);

    std::lock_guard lock(mutex);
    return std::make_shared<LocalQueryResultCacheWriter>(cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

void LocalQueryResultCache::clear(const std::optional<String> & tag)
{
    if (tag)
    {
        auto predicate = [tag](const Key & key, const Cache::MappedPtr &) { return key.tag == tag.value(); };
        cache.remove(predicate);
    }
    else
    {
        cache.clear();
    }

    std::lock_guard lock(mutex);
    times_executed.clear();
}

size_t LocalQueryResultCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t LocalQueryResultCache::count() const
{
    return cache.count();
}

size_t LocalQueryResultCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    static constexpr auto TIMES_EXECUTED_MAX_SIZE = 10'000uz;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

std::vector<QueryResultCache::KeyMapped> LocalQueryResultCache::dump() const
{
    auto content = cache.dump();
    std::vector<QueryResultCache::KeyMapped> ret;
    ret.reserve(content.size());
    for (auto & c : content)
        ret.emplace_back(c.key, c.mapped);
    return ret;
}

void registerLocalQueryResultCache(QueryResultCacheFactory & factory)
{
    factory.registerQueryResultCache(DEFAULT_QUERY_RESULT_CACHE_TYPE, [] (size_t max_cache_size, const Poco::Util::AbstractConfiguration & config) {
        size_t query_result_cache_max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
        if (query_result_cache_max_size_in_bytes > max_cache_size)
            query_result_cache_max_size_in_bytes = max_cache_size;
        size_t query_result_cache_max_entries = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
        size_t query_result_cache_max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t query_result_cache_max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
        return std::make_shared<LocalQueryResultCache>(query_result_cache_max_size_in_bytes, query_result_cache_max_entries, query_result_cache_max_entry_size_in_bytes, query_result_cache_max_entry_size_in_rows);
    });
}

}
