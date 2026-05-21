#pragma once

#include <Common/IRemoteCacheBackend.h>
#include <Common/logger_useful.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/QueryResultCacheRedisKey.h>
#include <Storages/RedisCommon.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>

namespace DB
{

/// Redis-backed implementation of IRemoteCacheBackend for QueryResultCache Key/Entry pairs.
///
/// Key encoding:   `ch:qcache:v{global_generation}:t{tag_generation}:{tag}:{scope}:{ast_hash.high64:016x}{ast_hash.low64:016x}`
/// Value encoding: Key::serializeTo() + Entry::serializeTo()
///
/// Lua scripts are loaded lazily on first use and their SHAs are cached.
/// On NOSCRIPT errors the scripts are reloaded and the operation is retried once.
class RedisRemoteCacheBackend
    : public IRemoteCacheBackend<QueryResultCache::Key, QueryResultCache::Entry>
{
public:
    explicit RedisRemoteCacheBackend(
        RedisConfiguration config_,
        size_t max_entry_chunks_,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_);

    /// IRemoteCacheBackend interface
    std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
    getWithKey(const QueryResultCache::Key & key) override;

    std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
    getWithKey(const QueryResultCache::Key & key, const String & redis_key);

    void set(
        const QueryResultCache::Key & key,
        const QueryResultCache::Entry & value,
        std::chrono::milliseconds ttl) override;

    bool setIfValid(
        const QueryResultCache::Key & key,
        const QueryResultCache::Entry & value,
        const String & redis_key,
        std::chrono::milliseconds ttl,
        const QueryResultCache::WriteContext & write_context,
        const String & lock_token);

    void remove(const QueryResultCache::Key & key) override;
    void clearByTag(const String & tag) override;
    void clear() override;

    std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
    dump(size_t max_keys) override;

    size_t count() override;

    /// Update the per-entry size bounds applied during deserialization.
    /// Used when the cache configuration is reloaded at runtime.
    void setEntrySizeLimits(size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_);

    /// Atomic lock acquisition: `SET redis_key <token> NX PX ttl_ms`.
    /// Returns the unique lock token on success, or an empty string on failure.
    /// Never throws (degrades gracefully).
    String tryAcquireLock(const std::string & redis_key, std::chrono::milliseconds ttl);

    /// Release the lock via Lua compare-and-delete: only deletes the key
    /// if the stored value matches the given token. Best-effort, never throws.
    void releaseLock(const std::string & redis_key, const String & token);

    /// Renew the lock TTL if the token still matches.
    bool renewLock(const std::string & redis_key, const String & token, std::chrono::milliseconds ttl);

    /// Returns true if the lock key exists. Never throws (returns false on error).
    bool lockExists(const std::string & redis_key);

    /// Atomically check for an existing entry and, if missing, try to acquire
    /// the stampede lock. Reduces 2 RTTs (GET + SET NX) to 1 RTT.
    ///
    /// Returns:
    ///   {1, data_string} — entry exists, data contains the serialized value
    ///   {2, ""}          — no entry, lock acquired with the given token
    ///   {0, ""}          — no entry, lock held by another node
    struct GetOrLockResult
    {
        int status;        /// 0 = lock_held_by_other, 1 = data_found, 2 = lock_acquired
        std::string data;  /// non-empty only when status == 1
    };
    GetOrLockResult getOrTryAcquireLock(
        const String & redis_key,
        const std::string & lock_key,
        const String & token,
        std::chrono::milliseconds lock_ttl);

    QueryResultCache::WriteContext getWriteContext(const String & tag);
    void bumpGeneration(const std::optional<String> & tag);
    void clearWithGenerationBump(const std::optional<String> & tag);

private:
    /// Borrow a Redis connection from the pool, creating it lazily if needed.
    RedisConnectionPtr borrowConnection();

    /// Load all Lua scripts if not yet loaded (or reload after NOSCRIPT). Thread-safe.
    void ensureScriptsLoaded(Poco::Redis::Client & client);

    struct ScriptShas
    {
        std::string set;
        std::string clear_by_tag;
        std::string release_lock;
        std::string renew_lock;
        std::string dump;
        std::string count;
        std::string set_if_valid;
        std::string get_or_lock;
        std::string clear_and_bump;
    };

    /// Copy script SHAs under `scripts_mutex` so callers can build commands
    /// without racing concurrent NOSCRIPT reload paths.
    ScriptShas getScriptShas() const;

    /// Execute a Redis command with automatic NOSCRIPT retry. If the first attempt
    /// fails with NOSCRIPT (Redis was restarted and lost our scripts), reload all
    /// Lua scripts and retry once.
    ///
    /// `build_and_execute` receives a `const ScriptShas &` and must build + execute
    /// the command, returning the result. It is called once normally, and once more
    /// after script reload on NOSCRIPT.
    template <typename F>
    auto executeWithNoscriptRetry(Poco::Redis::Client & client, F && build_and_execute)
        -> decltype(build_and_execute(std::declval<const ScriptShas &>()));

    template <typename F>
    auto execute(F && operation)
        -> decltype(operation(std::declval<Poco::Redis::Client &>()));

    /// Serialize key + entry into a single binary string (Redis value).
    static std::string serializeValue(
        const QueryResultCache::Key & key,
        const QueryResultCache::Entry & entry);

    /// Deserialize a Redis value binary string back to Key + Entry.
    /// `max_entry_size_in_bytes` / `max_entry_size_in_rows` bound the decoded chunks
    /// to protect against oversized payloads in the external Redis store.
    static std::pair<QueryResultCache::Key, QueryResultCache::Entry>
    deserializeValue(
        const std::string & data,
        size_t max_entry_chunks,
        size_t max_entry_size_in_bytes,
        size_t max_entry_size_in_rows);

    RedisConfiguration config;
    size_t max_entry_chunks;
    /// Atomic because they may be updated via `setEntrySizeLimits` while a deserialization is in flight.
    std::atomic<size_t> max_entry_size_in_bytes;
    std::atomic<size_t> max_entry_size_in_rows;
    RedisPoolPtr getPoolForEndpoint(const RedisEndpoint & endpoint);

    mutable std::mutex pools_mutex;
    std::unordered_map<RedisEndpoint, RedisPoolPtr, RedisEndpointHash> pools TSA_GUARDED_BY(pools_mutex);

    LoggerPtr logger = getLogger("RedisRemoteCacheBackend");

    /// Lua script bodies
    static constexpr std::string_view SET_SCRIPT =
        "if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then "
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

    /// Compare-and-delete: only release the lock if the stored value matches our token.
    /// Prevents a slow writer from deleting a lock that was re-acquired by another node
    /// after the original lock's TTL expired.
    static constexpr std::string_view RELEASE_LOCK_SCRIPT =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "    return redis.call('DEL', KEYS[1]) "
        "else "
        "    return 0 "
        "end";

    static constexpr std::string_view RENEW_LOCK_SCRIPT =
        "if redis.call('GET', KEYS[1]) == ARGV[1] then "
        "    return redis.call('PEXPIRE', KEYS[1], ARGV[2]) "
        "else "
        "    return 0 "
        "end";

    static constexpr std::string_view DUMP_SCRIPT =
        "local cursor = '0' "
        "local result = {} "
        "local max_keys = tonumber(ARGV[2]) "
        "local unlimited = (not max_keys) or max_keys <= 0 "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 100) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    for _, k in ipairs(keys) do "
        "        if (not unlimited) and #result >= max_keys * 2 then break end "
        "        local v = redis.call('GET', k) "
        "        if v then "
        "            table.insert(result, k) "
        "            table.insert(result, v) "
        "        end "
        "    end "
        "until cursor == '0' or ((not unlimited) and #result >= max_keys * 2) "
        "return result";

    static constexpr std::string_view COUNT_SCRIPT =
        "local cursor = '0' "
        "local result = 0 "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', 1000) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    result = result + #keys "
        "until cursor == '0' "
        "return result";

    static constexpr std::string_view SET_IF_VALID_SCRIPT =
        "local lock_value = redis.call('GET', KEYS[2]) "
        "if lock_value ~= ARGV[1] then "
        "    return 0 "
        "end "
        "local global_generation = tonumber(redis.call('GET', KEYS[3]) or '0') "
        "if global_generation ~= tonumber(ARGV[2]) then "
        "    return 0 "
        "end "
        "local tag_generation = tonumber(redis.call('GET', KEYS[4]) or '0') "
        "if tag_generation ~= tonumber(ARGV[3]) then "
        "    return 0 "
        "end "
        "redis.call('SET', KEYS[1], ARGV[4], 'PX', ARGV[5]) "
        "redis.call('DEL', KEYS[2]) "
        "return 1";

    /// Atomically GET the data key; if it exists return it, otherwise try to
    /// acquire the stampede lock via SET NX PX. Combines the GET + SET NX
    /// pair from `hasNonStaleEntry` into a single RTT.
    ///
    /// KEYS[1] = data key, KEYS[2] = lock key
    /// ARGV[1] = lock token, ARGV[2] = lock ttl ms
    /// Returns: {status, data_or_empty}
    ///   status=1: data key exists, data returned
    ///   status=2: lock acquired successfully
    ///   status=0: lock already held by another node
    static constexpr std::string_view GET_OR_LOCK_SCRIPT =
        "local data = redis.call('GET', KEYS[1]) "
        "if data then "
        "    return {1, data} "
        "end "
        "if redis.call('SET', KEYS[2], ARGV[1], 'NX', 'PX', ARGV[2]) then "
        "    return {2, ''} "
        "end "
        "return {0, ''}";

    static constexpr std::string_view CLEAR_AND_BUMP_SCRIPT =
        "local cursor = '0' "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[1], 'COUNT', ARGV[3]) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    if #keys > 0 then "
        "        redis.call('DEL', unpack(keys)) "
        "    end "
        "until cursor == '0' "
        "cursor = '0' "
        "repeat "
        "    local res = redis.call('SCAN', cursor, 'MATCH', ARGV[2], 'COUNT', ARGV[3]) "
        "    cursor = res[1] "
        "    local keys = res[2] "
        "    if #keys > 0 then "
        "        redis.call('DEL', unpack(keys)) "
        "    end "
        "until cursor == '0' "
        "return redis.call('INCR', KEYS[1])";

    /// SHA-1 hashes of loaded scripts, empty until first load.
    mutable std::mutex scripts_mutex;
    std::string sha_set;
    std::string sha_clear_by_tag;
    std::string sha_release_lock;
    std::string sha_renew_lock;
    std::string sha_dump;
    std::string sha_count;
    std::string sha_set_if_valid;
    std::string sha_get_or_lock;
    std::string sha_clear_and_bump;
    bool scripts_loaded = false;

};

}
