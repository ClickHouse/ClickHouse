#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/RemoteQueryResultCache.h>

#include <Common/Exception.h>
#include <Common/StringUtils.h>
#include <base/find_symbols.h>
#include <Core/Defines.h>
#include <Storages/RedisCommon.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/// Register the default in-process LRU query result cache.
static void registerLocalQueryResultCache(QueryResultCacheFactory & factory)
{
    factory.registerCache("local", [](const Poco::Util::AbstractConfiguration & config)
    {
        size_t max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
        size_t max_entries = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
        size_t max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_size_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
        return std::make_shared<LocalQueryResultCache>(max_size_in_bytes, max_entries, max_entry_size_in_bytes, max_entry_size_in_rows);
    });
}

/// Register the Redis-backed shared query result cache with
/// anti-stampede lock configuration.
static void registerRemoteQueryResultCache(QueryResultCacheFactory & factory)
{
    factory.registerCache("redis", [](const Poco::Util::AbstractConfiguration & config)
    {
        RedisConfiguration redis_cfg;
        redis_cfg.host = config.getString("query_cache.redis.host", "127.0.0.1");
        redis_cfg.port = static_cast<uint32_t>(config.getUInt("query_cache.redis.port", 6379));
        redis_cfg.password = config.getString("query_cache.redis.password", DEFAULT_REDIS_PASSWORD);
        redis_cfg.db_index = static_cast<uint32_t>(config.getUInt("query_cache.redis.db_index", DEFAULT_REDIS_DB_INDEX));
        redis_cfg.pool_size = static_cast<uint32_t>(config.getUInt("query_cache.redis.pool_size", DEFAULT_REDIS_POOL_SIZE));
        redis_cfg.connect_timeout_ms = static_cast<uint32_t>(config.getUInt("query_cache.redis.connect_timeout_ms", DEFAULT_REDIS_CONNECT_TIMEOUT_MS));
        redis_cfg.receive_timeout_ms = static_cast<uint32_t>(config.getUInt("query_cache.redis.receive_timeout_ms", DEFAULT_REDIS_RECEIVE_TIMEOUT_MS));
        redis_cfg.max_retries = static_cast<uint32_t>(config.getUInt("query_cache.redis.max_retries", DEFAULT_REDIS_MAX_RETRIES));
        redis_cfg.retry_delay_ms = static_cast<uint32_t>(config.getUInt("query_cache.redis.retry_delay_ms", DEFAULT_REDIS_RETRY_DELAY_MS));
        /// storage_type is unused by the cache backend; set a neutral value.
        redis_cfg.storage_type = RedisStorageType::SIMPLE;

        if (config.has("query_cache.redis.topology_mode"))
        {
            const auto topology_mode = boost::to_lower_copy(config.getString("query_cache.redis.topology_mode"));
            if (topology_mode != "standalone")
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Only `standalone` is supported for `query_cache.redis.topology_mode`, got: {}",
                    topology_mode);
        }

        if (config.has("query_cache.redis.startup_nodes"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`query_cache.redis.startup_nodes` is not supported for query cache Redis backend");

        size_t max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_size_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
        size_t max_entry_chunks = config.getUInt64("query_cache.max_entry_chunks", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_CHUNKS);

        /// Anti-stampede lock configuration.
        size_t lock_ttl_ms = config.getUInt64("query_cache.redis.lock_ttl_ms", 30'000);
        size_t lock_poll_interval_ms = config.getUInt64("query_cache.redis.lock_poll_interval_ms", 200);
        size_t lock_max_wait_ms = config.getUInt64("query_cache.redis.lock_max_wait_ms", lock_ttl_ms);

        return std::make_shared<RemoteQueryResultCache>(
            std::move(redis_cfg),
            max_entry_size_in_bytes,
            max_entry_size_in_rows,
            max_entry_chunks,
            std::chrono::milliseconds(lock_ttl_ms),
            std::chrono::milliseconds(lock_poll_interval_ms),
            std::chrono::milliseconds(lock_max_wait_ms));
    });
}

void registerQueryResultCaches(QueryResultCacheFactory & factory)
{
    registerLocalQueryResultCache(factory);
    registerRemoteQueryResultCache(factory);
}

}
