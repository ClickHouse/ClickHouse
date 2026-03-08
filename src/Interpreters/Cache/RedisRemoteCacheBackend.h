#pragma once

#include <Common/IRemoteCacheBackend.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Storages/RedisCommon.h>

#include <mutex>
#include <string>

namespace DB
{

/// Redis-backed implementation of IRemoteCacheBackend for QueryResultCache Key/Entry pairs.
///
/// Key encoding:   {tag}{ast_hash.high64:016x}{ast_hash.low64:016x}
/// Value encoding: Key::serializeTo() + Entry::serializeTo()
///
/// Lua scripts are loaded lazily on first use and their SHAs are cached.
/// On NOSCRIPT errors the scripts are reloaded and the operation is retried once.
class RedisRemoteCacheBackend
    : public IRemoteCacheBackend<QueryResultCache::Key, QueryResultCache::Entry>
{
public:
    explicit RedisRemoteCacheBackend(RedisConfiguration config_);

    /// IRemoteCacheBackend interface
    std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
    getWithKey(const QueryResultCache::Key & key) override;

    void set(
        const QueryResultCache::Key & key,
        const QueryResultCache::Entry & value,
        std::chrono::milliseconds ttl) override;

    void remove(const QueryResultCache::Key & key) override;
    void clearByTag(const String & tag) override;
    void clear() override;

    std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
    dump(size_t max_keys) override;

    size_t count() override;

    /// Atomic lock acquisition: `SET redis_key "IN_PROGRESS" NX PX ttl_ms`.
    /// Returns true if the lock was acquired. Never throws (degrades gracefully).
    bool tryAcquireLock(const std::string & redis_key, std::chrono::milliseconds ttl);

    /// Release the lock: `DEL redis_key`. Best-effort, never throws.
    void releaseLock(const std::string & redis_key);

    /// Returns true if the lock key exists. Never throws (returns false on error).
    bool lockExists(const std::string & redis_key);

private:
    /// Borrow a Redis connection from the pool, creating it lazily if needed.
    RedisConnectionPtr borrowConnection();

    /// Load all Lua scripts if not yet loaded (or reload after NOSCRIPT). Thread-safe.
    void ensureScriptsLoaded(Poco::Redis::Client & client);

    /// Serialize key + entry into a single binary string (Redis value).
    static std::string serializeValue(
        const QueryResultCache::Key & key,
        const QueryResultCache::Entry & entry);

    /// Deserialize a Redis value binary string back to Key + Entry.
    static std::pair<QueryResultCache::Key, QueryResultCache::Entry>
    deserializeValue(const std::string & data);

    RedisConfiguration config;
    RedisPoolPtr pool;

    LoggerPtr logger = getLogger("RedisRemoteCacheBackend");

    /// Lua script bodies
    static constexpr std::string_view SET_SCRIPT =
        "if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then "
        "    redis.call('pexpire', KEYS[1], ARGV[2]) "
        "    return 1 "
        "else "
        "    return 0 "
        "end";

    static constexpr std::string_view CLEAR_BY_TAG_SCRIPT =
        "local cursor = '0' "
        "local deleted = 0 "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', ARGV[2]) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    if #keys > 0 then "
        "        redis.call('DEL', unpack(keys)) "
        "        deleted = deleted + #keys "
        "    end "
        "until cursor == '0' "
        "return deleted";

    static constexpr std::string_view DUMP_SCRIPT =
        "local cursor = '0' "
        "local result = {} "
        "local max_keys = tonumber(ARGV[2]) "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 100) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    for _, k in ipairs(keys) do "
        "        if #result >= max_keys then break end "
        "        local v = redis.call('GET', k) "
        "        if v then "
        "            table.insert(result, k) "
        "            table.insert(result, v) "
        "        end "
        "    end "
        "until cursor == '0' or #result >= max_keys * 2 "
        "return result";

    /// SHA-1 hashes of loaded scripts, empty until first load.
    mutable std::mutex scripts_mutex;
    std::string sha_set;
    std::string sha_clear_by_tag;
    std::string sha_dump;
    bool scripts_loaded = false;
};

}
