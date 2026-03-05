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

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

RedisRemoteCacheBackend::RedisRemoteCacheBackend(RedisConfiguration config_)
    : config(std::move(config_))
{
    pool = std::make_shared<RedisPool>(config.pool_size);
}

// ---------------------------------------------------------------------------
// Connection management
// ---------------------------------------------------------------------------

RedisConnectionPtr RedisRemoteCacheBackend::borrowConnection()
{
    return getRedisConnection(pool, config);
}

// ---------------------------------------------------------------------------
// Lua script loading
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Serialization helpers
// ---------------------------------------------------------------------------

std::string RedisRemoteCacheBackend::serializeValue(
    const QueryResultCache::Key & key,
    const QueryResultCache::Entry & entry)
{
    WriteBufferFromOwnString buf;
    key.serializeTo(buf);
    entry.serializeTo(buf, *key.header);
    return std::move(buf.str());
}

std::pair<QueryResultCache::Key, QueryResultCache::Entry>
RedisRemoteCacheBackend::deserializeValue(const std::string & data)
{
    ReadBufferFromString buf(data);
    auto key = QueryResultCache::Key::deserializeFrom(buf);
    auto entry = QueryResultCache::Entry::deserializeFrom(buf, *key.header);
    return {std::move(key), std::move(entry)};
}

// ---------------------------------------------------------------------------
// getWithKey
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// set
// ---------------------------------------------------------------------------

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
    /// numkeys = 1
    cmd << "1";
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
            /// Redis was restarted and lost our scripts — reload and retry once.
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

// ---------------------------------------------------------------------------
// remove
// ---------------------------------------------------------------------------

void RedisRemoteCacheBackend::remove(const QueryResultCache::Key & key)
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("DEL");
    cmd << key.encodeToRedisKey();
    conn->client->execute<Poco::Int64>(cmd);
}

// ---------------------------------------------------------------------------
// clearByTag
// ---------------------------------------------------------------------------

void RedisRemoteCacheBackend::clearByTag(const String & tag)
{
    auto conn = borrowConnection();
    ensureScriptsLoaded(*conn->client);

    /// Pattern: {tag}* matches all keys with this tag prefix.
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

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------

void RedisRemoteCacheBackend::clear()
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("FLUSHDB");
    cmd << "ASYNC";
    conn->client->execute<std::string>(cmd);
}

// ---------------------------------------------------------------------------
// dump
// ---------------------------------------------------------------------------

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

    /// DUMP_SCRIPT returns a flat list: [key1, value1, key2, value2, ...]
    std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>> result;
    result.reserve(raw.size() / 2);

    for (size_t i = 0; i + 1 < raw.size(); i += 2)
    {
        const std::string & value_bytes = raw.get<Poco::Redis::BulkString>(i + 1).value();
        try
        {
            result.push_back(deserializeValue(value_bytes));
        }
        catch (...)
        {
            /// Skip malformed entries — don't break the entire dump.
            LOG_WARNING(logger, "Skipping malformed cache entry during dump: {}", getCurrentExceptionMessage(false));
        }
    }

    return result;
}

// ---------------------------------------------------------------------------
// count
// ---------------------------------------------------------------------------

size_t RedisRemoteCacheBackend::count()
{
    auto conn = borrowConnection();
    Poco::Redis::Command cmd("DBSIZE");
    return static_cast<size_t>(conn->client->execute<Poco::Int64>(cmd));
}

// ---------------------------------------------------------------------------
// tryAcquireLock
// ---------------------------------------------------------------------------

bool RedisRemoteCacheBackend::tryAcquireLock(const std::string & redis_key, std::chrono::milliseconds ttl)
try
{
    auto conn = borrowConnection();

    /// SET key value NX PX ttl_ms — atomic, returns "OK" on success or null on failure.
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

// ---------------------------------------------------------------------------
// releaseLock
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// lockExists
// ---------------------------------------------------------------------------

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
