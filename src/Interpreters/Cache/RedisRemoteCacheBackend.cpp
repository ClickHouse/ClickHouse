#include <Interpreters/Cache/RedisRemoteCacheBackend.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Redis/Command.h>
#include <Poco/Redis/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_QUERY_RESULT_CACHE_ERROR;
}

RedisRemoteCacheBackend::RedisRemoteCacheBackend(RedisConfiguration config_)
    : config(std::move(config_))
{
    pool = std::make_shared<RedisPool>(config.pool_size);
}

RedisConnectionPtr RedisRemoteCacheBackend::borrowConnection()
{
    return getRedisConnection(pool, config);
}

/// Load all Lua scripts into Redis if not yet loaded (or reload
/// after a NOSCRIPT error indicates Redis was restarted).
void RedisRemoteCacheBackend::ensureScriptsLoaded(Poco::Redis::Client & client)
{
    std::lock_guard lock(scripts_mutex);
    if (scripts_loaded)
        return;

    auto load = [&](std::string_view body) -> std::string
    {
        Poco::Redis::Command cmd("SCRIPT");
        cmd << "LOAD" << std::string(body);
        return client.execute<Poco::Redis::BulkString>(cmd).value();
    };

    sha_set           = load(SET_SCRIPT);
    sha_clear_by_tag  = load(CLEAR_BY_TAG_SCRIPT);
    sha_dump          = load(DUMP_SCRIPT);
    scripts_loaded    = true;
}

/// Concatenate the serialized key and entry into a single binary
/// string suitable for storing as a Redis value.
std::string RedisRemoteCacheBackend::serializeValue(
    const QueryResultCache::Key & key,
    const QueryResultCache::Entry & entry)
{
    WriteBufferFromOwnString buf;
    key.serializeTo(buf);
    entry.serializeTo(buf, *key.header);
    return std::move(buf.str());
}

/// Reconstruct a Key + Entry pair from the binary Redis value
/// produced by `serializeValue`.
std::pair<QueryResultCache::Key, QueryResultCache::Entry>
RedisRemoteCacheBackend::deserializeValue(const std::string & data)
{
    ReadBufferFromString buf(data);
    auto key = QueryResultCache::Key::deserializeFrom(buf);
    auto entry = QueryResultCache::Entry::deserializeFrom(buf, *key.header);
    return {std::move(key), std::move(entry)};
}

/// Issue a `GET` command to Redis. Returns nullopt on miss or on
/// any error (graceful degradation).
std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
RedisRemoteCacheBackend::getWithKey(const QueryResultCache::Key & key)
try
{
    auto conn = borrowConnection();
    ensureScriptsLoaded(*conn->client);

    const std::string redis_key = key.encodeToRedisKey();
    Poco::Redis::Command cmd("GET");
    cmd << redis_key;

    auto result = conn->client->execute<Poco::Redis::BulkString>(cmd);
    if (result.isNull())
        return std::nullopt;

    return deserializeValue(result.value());
}
catch (...)
{
    LOG_ERROR(logger, "Failed to read from Redis: {}", getCurrentExceptionMessage(false));
    return std::nullopt;
}

/// Use the `SET_SCRIPT` Lua script to atomically set the key only
/// if it does not already exist, with a TTL in milliseconds.
/// Retries once on NOSCRIPT (Redis restart).
void RedisRemoteCacheBackend::set(
    const QueryResultCache::Key & key,
    const QueryResultCache::Entry & value,
    std::chrono::milliseconds ttl)
try
{
    if (ttl.count() <= 0)
        return; /// Entry has already expired, no point writing it.

    auto conn = borrowConnection();
    ensureScriptsLoaded(*conn->client);

    const std::string redis_key = key.encodeToRedisKey();
    const std::string serialized = serializeValue(key, value);
    const std::string ttl_ms_str = std::to_string(ttl.count());

    Poco::Redis::Command cmd("EVALSHA");
    cmd << sha_set;
    cmd << "1"; /// numkeys
    cmd << redis_key;
    cmd << serialized;
    cmd << ttl_ms_str;

    try
    {
        conn->client->execute<Poco::Int64>(cmd);
    }
    catch (const Poco::Redis::RedisException & e)
    {
        if (std::string(e.what()).starts_with("NOSCRIPT"))
        {
            /// Redis lost our scripts — reload and retry once.
            {
                std::lock_guard lock(scripts_mutex);
                scripts_loaded = false;
            }
            ensureScriptsLoaded(*conn->client);
            cmd.clear();
            cmd << "EVALSHA" << sha_set << "1" << redis_key << serialized << ttl_ms_str;
            conn->client->execute<Poco::Int64>(cmd);
        }
        else
        {
            throw;
        }
    }
}
catch (...)
{
    LOG_ERROR(logger, "Failed to write to Redis: {}", getCurrentExceptionMessage(false));
    /// Swallow — cache write failures must not interrupt the query.
}

/// Delete a single entry by its encoded Redis key.
void RedisRemoteCacheBackend::remove(const QueryResultCache::Key & key)
try
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("DEL");
    cmd << key.encodeToRedisKey();
    conn->client->execute<Poco::Int64>(cmd);
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to remove entry from external query result cache: {}", getCurrentExceptionMessage(false));
}

/// Use the `CLEAR_BY_TAG_SCRIPT` Lua script to SCAN and delete
/// all keys matching the `{tag}*` pattern.
void RedisRemoteCacheBackend::clearByTag(const String & tag)
try
{
    auto conn = borrowConnection();
    ensureScriptsLoaded(*conn->client);

    const std::string pattern = tag + "*";

    Poco::Redis::Command cmd("EVALSHA");
    cmd << sha_clear_by_tag;
    cmd << "0"; /// no KEYS[], only ARGV[]
    cmd << pattern;
    cmd << "100"; /// batch size per SCAN iteration

    try
    {
        conn->client->execute<Poco::Int64>(cmd);
    }
    catch (const Poco::Redis::RedisException & e)
    {
        if (std::string(e.what()).starts_with("NOSCRIPT"))
        {
            {
                std::lock_guard lock(scripts_mutex);
                scripts_loaded = false;
            }
            ensureScriptsLoaded(*conn->client);
            cmd.clear();
            cmd << "EVALSHA" << sha_clear_by_tag << "0" << pattern << "100";
            conn->client->execute<Poco::Int64>(cmd);
        }
        else
        {
            throw;
        }
    }
}
catch (const Exception &)
{
    throw;
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to clear external query result cache by tag '{}': {}", tag, getCurrentExceptionMessage(false));
}

/// Issue `FLUSHDB ASYNC` to drop all keys in the selected database.
void RedisRemoteCacheBackend::clear()
try
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("FLUSHDB");
    cmd << "ASYNC";
    conn->client->execute<std::string>(cmd);
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to clear external query result cache: {}", getCurrentExceptionMessage(false));
}

/// Use the `DUMP_SCRIPT` Lua script to SCAN all keys and return
/// up to `max_keys` key-value pairs. Lock keys (ending with
/// `:lock`) and malformed entries are silently skipped.
std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
RedisRemoteCacheBackend::dump(size_t max_keys)
{
    auto conn = borrowConnection();
    ensureScriptsLoaded(*conn->client);

    Poco::Redis::Command cmd("EVALSHA");
    cmd << sha_dump;
    cmd << "0";       /// no KEYS[]
    cmd << "*";       /// match all keys
    cmd << std::to_string(max_keys);

    Poco::Redis::Array raw;
    try
    {
        raw = conn->client->execute<Poco::Redis::Array>(cmd);
    }
    catch (const Poco::Redis::RedisException & e)
    {
        if (std::string(e.what()).starts_with("NOSCRIPT"))
        {
            {
                std::lock_guard lock(scripts_mutex);
                scripts_loaded = false;
            }
            ensureScriptsLoaded(*conn->client);
            cmd.clear();
            cmd << "EVALSHA" << sha_dump << "0" << "*" << std::to_string(max_keys);
            raw = conn->client->execute<Poco::Redis::Array>(cmd);
        }
        else
        {
            throw;
        }
    }

    /// The Lua script returns a flat list: [key1, val1, key2, val2, ...]
    std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>> result;
    result.reserve(raw.size() / 2);

    for (size_t i = 0; i + 1 < raw.size(); i += 2)
    {
        /// Skip stampede lock keys — they are not cache entries.
        const std::string & redis_key = raw.get<Poco::Redis::BulkString>(i).value();
        if (redis_key.ends_with(":lock"))
            continue;

        const std::string & value_bytes = raw.get<Poco::Redis::BulkString>(i + 1).value();
        try
        {
            result.push_back(deserializeValue(value_bytes));
        }
        catch (...)
        {
            LOG_WARNING(logger, "Skipping malformed cache entry during dump: {}", getCurrentExceptionMessage(false));
        }
    }

    return result;
}

/// Return the total number of keys in the selected Redis database.
size_t RedisRemoteCacheBackend::count()
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("DBSIZE");
    return static_cast<size_t>(conn->client->execute<Poco::Int64>(cmd));
}

/// Atomically acquire a lock using `SET key "IN_PROGRESS" NX PX
/// ttl_ms`. Returns true if the lock was acquired.
bool RedisRemoteCacheBackend::tryAcquireLock(const std::string & redis_key, std::chrono::milliseconds ttl)
try
{
    auto conn = borrowConnection();

    Poco::Redis::Command cmd("SET");
    cmd << redis_key << "IN_PROGRESS" << "NX" << "PX" << std::to_string(ttl.count());

    auto reply = conn->client->execute<Poco::Redis::BulkString>(cmd);
    return !reply.isNull();
}
catch (...)
{
    LOG_WARNING(logger, "Failed to acquire lock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return false;
}

/// Best-effort lock release via `DEL`. Never throws.
void RedisRemoteCacheBackend::releaseLock(const std::string & redis_key)
try
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("DEL");
    cmd << redis_key;
    conn->client->execute<Poco::Int64>(cmd);
}
catch (...)
{
    LOG_WARNING(logger, "Failed to release lock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
}

/// Check lock existence via `EXISTS`. Returns false on error.
bool RedisRemoteCacheBackend::lockExists(const std::string & redis_key)
try
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("EXISTS");
    cmd << redis_key;
    return conn->client->execute<Poco::Int64>(cmd) > 0;
}
catch (...)
{
    LOG_WARNING(logger, "Failed to check lock existence for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return false;
}

}
