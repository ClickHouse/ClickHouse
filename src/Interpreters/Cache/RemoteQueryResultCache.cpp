#include <Interpreters/Cache/RemoteQueryResultCache.h>
#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Parsers/TokenIterator.h>
#include <Common/ProfileEvents.h>
#include <Common/CurrentMetrics.h>
#include <Common/SipHash.h>
#include <Common/TTLCachePolicy.h>
#include <Core/Settings.h>
#include <Common/RedisCommon.h>


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

namespace ErrorCodes
{
    extern const int UNSUPPORTED_DROP_QUERY_CACHE_BY_TAG;
}

RemoteQueryResultCache::RemoteQueryResultCache(std::shared_ptr<RedisConfiguration> config, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
    : cache(config)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
{
}

void RemoteQueryResultCache::updateConfigurationImpl(std::shared_ptr<RedisConfiguration> config, size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    std::lock_guard lock(mutex);
    cache.updateConfiguration(config);
    max_entry_size_in_bytes = max_entry_size_in_bytes_;
    max_entry_size_in_rows = max_entry_size_in_rows_;
}

void RemoteQueryResultCache::updateConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto cache_config = std::make_shared<RedisConfiguration>();
    cache_config->host = config.getString("query_cache.redis.host", "");
    cache_config->port = config.getInt("query_cache.redis.port", 6379);
    cache_config->password = config.getString("query_cache.redis.password", "");
    cache_config->db_index = config.getInt("query_cache.redis.db_index", 0);
    cache_config->pool_size = config.getInt("query_cache.redis.pool_size", 32);
    cache_config->connect_timeout_ms = config.getInt("query_cache.redis.connect_timeout_ms", 500);
    cache_config->operation_timeout_ms = config.getInt("query_cache.redis.operation_timeout_ms", 500);

    size_t query_result_cache_max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
    size_t query_result_cache_max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
    updateConfigurationImpl(cache_config, query_result_cache_max_entry_size_in_bytes, query_result_cache_max_entry_size_in_rows);
}

std::shared_ptr<QueryResultCacheReader> RemoteQueryResultCache::createReader(const Key & key)
{
    std::lock_guard lock(mutex);
    return std::make_shared<RemoteQueryResultCacheReader>(cache, key, lock);
}

std::shared_ptr<QueryResultCacheWriter> RemoteQueryResultCache::createWriter(
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
    return std::make_shared<RemoteQueryResultCacheWriter>(cache, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

void RemoteQueryResultCache::clear(const std::optional<String> & tag)
{
    if (tag)
        cache.clearByTag(*tag);
    else
        cache.clear();

    std::lock_guard lock(mutex);
    times_executed.clear();
}

size_t RemoteQueryResultCache::sizeInBytes() const
{
    return cache.sizeInBytes();
}

size_t RemoteQueryResultCache::count() const
{
    return cache.count();
}

size_t RemoteQueryResultCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    // Regularly drop times_executed to avoid DOS-by-unlimited-growth.
    static constexpr auto TIMES_EXECUTED_MAX_SIZE = 10'000uz;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

std::vector<QueryResultCache::KeyMapped> RemoteQueryResultCache::dump() const
{
    auto content = cache.dump();
    std::vector<QueryResultCache::KeyMapped> ret;
    ret.reserve(content.size());
    for (auto & c : content)
        ret.emplace_back(c.key, c.mapped);
    return ret;
}

void registerRemoteQueryResultCache(QueryResultCacheFactory & factory)
{
    static auto constexpr remote_cache_name = "redis";
    factory.registerQueryResultCache(remote_cache_name, [] (size_t, const Poco::Util::AbstractConfiguration & config) {
        auto remote_cache_config = std::make_shared<RedisConfiguration>();
        remote_cache_config->host = config.getString("query_cache.redis.host", "");
        remote_cache_config->port = config.getInt("query_cache.redis.port", 6379);
        remote_cache_config->password = config.getString("query_cache.redis.password", "");
        remote_cache_config->db_index = config.getInt("query_cache.redis.db_index", 0);
        remote_cache_config->pool_size = config.getInt("query_cache.redis.pool_size", 32);
        remote_cache_config->connect_timeout_ms = config.getInt("query_cache.redis.connect_timeout_ms", 500);
        remote_cache_config->operation_timeout_ms = config.getInt("query_cache.redis.operation_timeout_ms", 500);
        size_t query_result_cache_max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t query_result_cache_max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
        return std::make_shared<RemoteQueryResultCache>(remote_cache_config, query_result_cache_max_entry_size_in_bytes, query_result_cache_max_entry_size_in_rows);
    });
}

}
