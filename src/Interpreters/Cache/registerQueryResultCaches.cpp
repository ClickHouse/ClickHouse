#include <Interpreters/Cache/QueryResultCacheFactory.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/RemoteQueryResultCache.h>

#include <Core/Defines.h>
#include <Storages/RedisCommon.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

/// Register the default in-process LRU query result cache.
static void registerLocalQueryResultCache(QueryResultCacheFactory & factory)
{
    factory.registerCache("local", [](const Poco::Util::AbstractConfiguration & config)
    {
        size_t max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
        size_t max_entries = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
        size_t max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
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
        /// storage_type is unused by the cache backend; set a neutral value.
        redis_cfg.storage_type = RedisStorageType::SIMPLE;

        size_t max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
        size_t max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);

        /// Limit how many entries dump() will fetch from Redis for system.query_cache.
        size_t max_entries_for_dump = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);

        /// Anti-stampede lock configuration.
        size_t lock_ttl_ms = config.getUInt64("query_cache.redis.lock_ttl_ms", 30'000);
        size_t lock_poll_interval_ms = config.getUInt64("query_cache.redis.lock_poll_interval_ms", 200);
        size_t lock_max_wait_ms = config.getUInt64("query_cache.redis.lock_max_wait_ms", lock_ttl_ms);

        return std::make_shared<RemoteQueryResultCache>(
            std::move(redis_cfg),
            max_entry_size_in_bytes,
            max_entry_size_in_rows,
            max_entries_for_dump,
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
