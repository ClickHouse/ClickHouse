#include <Interpreters/Cache/RedisRemoteCacheBackend.h>

#include <Common/Exception.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Poco/Redis/Command.h>
#include <Poco/Redis/Exception.h>

#include <algorithm>
#include <array>
#include <exception>

#include <Poco/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXTERNAL_QUERY_RESULT_CACHE_ERROR;
    extern const int TOO_LARGE_ARRAY_SIZE;
}

namespace
{

/// Redis SCAN MATCH uses glob patterns, not regex.
constexpr std::string_view QUERY_CACHE_HASH_GLOB = "????????????????????????????????";

/// Escape Redis glob metacharacters (`*`, `?`, `[`, `]`, `\`) in a string
/// so it can be safely used as a literal in SCAN MATCH patterns.
std::string escapeRedisGlob(const String & s)
{
    std::string result;
    result.reserve(s.size());
    for (char c : s)
    {
        if (c == '*' || c == '?' || c == '[' || c == ']' || c == '\\')
            result += '\\';
        result += c;
    }
    return result;
}

std::string dataKeysPatternForTag(const String & tag)
{
    return std::string(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX) + "v*:t*:" + escapeRedisGlob(tag) + ":*:" + std::string(QUERY_CACHE_HASH_GLOB);
}

std::string lockKeysPatternForTag(const String & tag)
{
    return std::string(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX) + "v*:t*:" + escapeRedisGlob(tag) + ":*:" + std::string(QUERY_CACHE_HASH_GLOB) + ":lock";
}

std::string dataKeysPatternForAllTags()
{
    return std::string(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX) + "v*:t*:*:*:" + std::string(QUERY_CACHE_HASH_GLOB);
}

std::string lockKeysPatternForAllTags()
{
    return std::string(QUERY_RESULT_CACHE_REDIS_KEY_PREFIX) + "v*:t*:*:*:" + std::string(QUERY_CACHE_HASH_GLOB) + ":lock";
}

/// `expected_tag` is forwarded to `CLEAR_BY_TAG_SCRIPT` so it can filter out
/// SCAN hits whose parsed tag does not match exactly. An empty string keeps
/// the all-tags semantics where every qcache-namespace match is in scope.
void deleteByPatternWithCachedScript(
    Poco::Redis::Client & client,
    const std::string & script_sha,
    const std::string & pattern,
    const std::string & expected_tag)
{
    Poco::Redis::Command cmd("EVALSHA");
    cmd << script_sha;
    cmd << "0"; /// no KEYS[], only ARGV[]
    cmd << pattern;
    cmd << "100"; /// batch size per SCAN iteration
    cmd << expected_tag;
    client.execute<Poco::Int64>(cmd);
}

std::string globalGenerationKey()
{
    return std::string(QUERY_RESULT_CACHE_REDIS_GENERATION_KEY_PREFIX) + "all";
}

std::string tagGenerationKey(const String & tag)
{
    return std::string(QUERY_RESULT_CACHE_REDIS_GENERATION_KEY_PREFIX) + "tag:" + tag;
}

}

RedisRemoteCacheBackend::RedisRemoteCacheBackend(
    RedisConfiguration config_,
    size_t max_entry_chunks_,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_)
    : config(std::move(config_))
    , max_entry_chunks(max_entry_chunks_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
{
}

void RedisRemoteCacheBackend::setEntrySizeLimits(
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_)
{
    max_entry_size_in_bytes.store(max_entry_size_in_bytes_, std::memory_order_relaxed);
    max_entry_size_in_rows.store(max_entry_size_in_rows_, std::memory_order_relaxed);
}

RedisConnectionPtr RedisRemoteCacheBackend::borrowConnection()
{
    const auto endpoint = getRedisStartupNodes(config).front();
    return getRedisConnection(getPoolForEndpoint(endpoint), config, endpoint);
}

RedisPoolPtr RedisRemoteCacheBackend::getPoolForEndpoint(const RedisEndpoint & endpoint)
{
    std::lock_guard lock(pools_mutex);
    auto it = pools.find(endpoint);
    if (it != pools.end())
        return it->second;

    auto pool = std::make_shared<RedisPool>(config.pool_size);
    pools.emplace(endpoint, pool);
    return pool;
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
    sha_release_lock  = load(RELEASE_LOCK_SCRIPT);
    sha_renew_lock    = load(RENEW_LOCK_SCRIPT);
    sha_dump          = load(DUMP_SCRIPT);
    sha_count         = load(COUNT_SCRIPT);
    sha_set_if_valid  = load(SET_IF_VALID_SCRIPT);
    sha_get_or_lock   = load(GET_OR_LOCK_SCRIPT);
    sha_clear_and_bump = load(CLEAR_AND_BUMP_SCRIPT);
    scripts_loaded    = true;
}

RedisRemoteCacheBackend::ScriptShas RedisRemoteCacheBackend::getScriptShas() const
{
    std::lock_guard lock(scripts_mutex);
    ScriptShas shas;
    shas.set = sha_set;
    shas.clear_by_tag = sha_clear_by_tag;
    shas.release_lock = sha_release_lock;
    shas.renew_lock = sha_renew_lock;
    shas.dump = sha_dump;
    shas.count = sha_count;
    shas.set_if_valid = sha_set_if_valid;
    shas.get_or_lock = sha_get_or_lock;
    shas.clear_and_bump = sha_clear_and_bump;
    return shas;
}

template <typename F>
auto RedisRemoteCacheBackend::executeWithNoscriptRetry(Poco::Redis::Client & client, F && build_and_execute)
    -> decltype(build_and_execute(std::declval<const ScriptShas &>()))
{
    ensureScriptsLoaded(client);
    auto shas = getScriptShas();
    try
    {
        return build_and_execute(shas);
    }
    catch (const Poco::Redis::RedisException & e)
    {
        if (!e.message().starts_with("NOSCRIPT"))
            throw;
        {
            std::lock_guard lock(scripts_mutex);
            scripts_loaded = false;
        }
        ensureScriptsLoaded(client);
        shas = getScriptShas();
        return build_and_execute(shas);
    }
}

template <typename F>
auto RedisRemoteCacheBackend::execute(F && operation)
    -> decltype(operation(std::declval<Poco::Redis::Client &>()))
{
    auto conn = borrowConnection();
    return operation(*conn->client);
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
/// produced by `serializeValue`. Bounds are passed through to
/// `Entry::deserializeFrom` so a malformed Redis payload cannot allocate
/// arbitrarily large memory before hitting the existing size checks.
std::pair<QueryResultCache::Key, QueryResultCache::Entry>
RedisRemoteCacheBackend::deserializeValue(
    const std::string & data,
    size_t max_entry_chunks,
    size_t max_entry_size_in_bytes,
    size_t max_entry_size_in_rows)
{
    /// Reject the entire blob if the raw serialized value already exceeds the configured byte
    /// budget. This short-circuits the per-chunk check below for the gross-corruption case.
    if (max_entry_size_in_bytes && data.size() > max_entry_size_in_bytes)
        throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
            "Cached query result blob exceeds max_entry_size_in_bytes: {} > {}",
            data.size(), max_entry_size_in_bytes);

    ReadBufferFromString buf(data);
    auto key = QueryResultCache::Key::deserializeFrom(buf);
    auto entry = QueryResultCache::Entry::deserializeFrom(
        buf, *key.header, max_entry_chunks, max_entry_size_in_bytes, max_entry_size_in_rows);
    return {std::move(key), std::move(entry)};
}

/// Issue a `GET` command to Redis. Returns nullopt on miss or on
/// any error (graceful degradation).
std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
RedisRemoteCacheBackend::getWithKey(const QueryResultCache::Key & key)
try
{
    return getWithKey(key, key.encodeToRedisKey());
}
catch (...)
{
    LOG_ERROR(logger, "Failed to read from Redis: {}", getCurrentExceptionMessage(false));
    return std::nullopt;
}

std::optional<std::pair<QueryResultCache::Key, QueryResultCache::Entry>>
RedisRemoteCacheBackend::getWithKey(const QueryResultCache::Key & key, const String & redis_key)
try
{
    auto result = execute([&](Poco::Redis::Client & client)
    {
        ensureScriptsLoaded(client);

        Poco::Redis::Command cmd("GET");
        cmd << redis_key;
        return client.execute<Poco::Redis::BulkString>(cmd);
    });

    if (result.isNull())
        return std::nullopt;

    auto deserialized = deserializeValue(result.value(), max_entry_chunks, max_entry_size_in_bytes, max_entry_size_in_rows);

    /// The Redis value is external and untrusted (corruption, manual writes, namespace collisions). Verify
    /// that the key embedded in the stored value actually matches the requested query before returning a hit,
    /// otherwise a value planted for a different AST could be served as the result of this query. On mismatch
    /// degrade to a cache miss rather than returning a wrong result.
    if (!(deserialized.first == key))
    {
        LOG_WARNING(logger, "Discarding Redis query cache value at key {} because the stored key does not match the requested query", redis_key);
        return std::nullopt;
    }

    return deserialized;
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

    const std::string redis_key = key.encodeToRedisKey();
    const std::string serialized = serializeValue(key, value);
    const std::string ttl_ms_str = std::to_string(ttl.count());

    execute([&](Poco::Redis::Client & client)
    {
        executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.set << "1" << redis_key << serialized << ttl_ms_str;
            client.execute<Poco::Int64>(cmd);
        });
    });
}
catch (...)
{
    LOG_ERROR(logger, "Failed to write to Redis: {}", getCurrentExceptionMessage(false));
    /// Swallow — cache write failures must not interrupt the query.
}

bool RedisRemoteCacheBackend::setIfValid(
    const QueryResultCache::Key & key,
    const QueryResultCache::Entry & value,
    const String & redis_key,
    std::chrono::milliseconds ttl,
    const QueryResultCache::WriteContext & write_context,
    const String & lock_token)
try
{
    if (ttl.count() <= 0)
        return false;

    const std::string lock_key = redis_key + ":lock";
    const std::string serialized = serializeValue(key, value);
    const std::string global_gen_key = globalGenerationKey();
    const std::string tag_gen_key = tagGenerationKey(key.tag);
    const std::string global_gen_str = std::to_string(write_context.global_generation);
    const std::string tag_gen_str = std::to_string(write_context.tag_generation);
    const std::string ttl_str = std::to_string(ttl.count());

    return execute([&](Poco::Redis::Client & client)
    {
        return executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.set_if_valid << "4" << redis_key << lock_key << global_gen_key << tag_gen_key
                << lock_token << global_gen_str << tag_gen_str << serialized << ttl_str;
            return client.execute<Poco::Int64>(cmd) != 0;
        });
    });
}
catch (...)
{
    LOG_ERROR(logger, "Failed to write to Redis with generation validation: {}", getCurrentExceptionMessage(false));
    return false;
}

/// Delete a single entry by its encoded Redis key.
void RedisRemoteCacheBackend::remove(const QueryResultCache::Key & key)
try
{
    execute([&](Poco::Redis::Client & client)
    {
        Poco::Redis::Command cmd("DEL");
        cmd << key.encodeToRedisKey();
        client.execute<Poco::Int64>(cmd);
    });
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to remove entry from external query result cache: {}", getCurrentExceptionMessage(false));
}

/// Use the `CLEAR_BY_TAG_SCRIPT` Lua script to SCAN candidates by glob and
/// then delete only those whose exact parsed tag matches `tag`. The glob
/// alone over-matches when a tag contains `:`, so the script-side exact
/// check is required to avoid clearing unrelated tags.
void RedisRemoteCacheBackend::clearByTag(const String & tag)
try
{
    const std::array patterns{
        dataKeysPatternForTag(tag),
        lockKeysPatternForTag(tag)};

    execute([&](Poco::Redis::Client & client)
    {
        executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            for (const auto & pattern : patterns)
                deleteByPatternWithCachedScript(client, shas.clear_by_tag, pattern, tag);
        });
    });
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

/// SCAN-and-delete all query-cache keys (data keys `{tag}:{hash}` and
/// lock keys `{tag}:{hash}:lock`).  Unlike `FLUSHDB`, this does not
/// touch unrelated keys that may reside in the same Redis database.
void RedisRemoteCacheBackend::clear()
try
{
    const std::array patterns{
        dataKeysPatternForAllTags(),
        lockKeysPatternForAllTags()};

    execute([&](Poco::Redis::Client & client)
    {
        executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            for (const auto & pattern : patterns)
                deleteByPatternWithCachedScript(client, shas.clear_by_tag, pattern, /*expected_tag=*/"");
        });
    });
}
catch (const Exception &)
{
    throw;
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
    const std::string pattern = dataKeysPatternForAllTags();
    const std::string max_keys_str = std::to_string(max_keys);

    auto raw = execute([&](Poco::Redis::Client & client)
    {
        return executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.dump << "0" << pattern << max_keys_str;
            return client.execute<Poco::Redis::Array>(cmd);
        });
    });

    /// The Lua script returns a flat list: [key1, val1, key2, val2, ...]
    std::vector<std::pair<QueryResultCache::Key, QueryResultCache::Entry>> result;
    if (raw.isNull())
        return result;

    result.reserve(raw.size() / 2);

    for (size_t i = 0; i + 1 < raw.size(); i += 2)
    {
        try
        {
            const auto & key_bs = raw.get<Poco::Redis::BulkString>(i);
            const auto & val_bs = raw.get<Poco::Redis::BulkString>(i + 1);

            /// Skip null entries (key expired between SCAN and GET).
            if (key_bs.isNull() || val_bs.isNull())
                continue;

            /// Skip stampede lock keys — they are not cache entries.
            if (key_bs.value().ends_with(":lock"))
                continue;

            result.push_back(deserializeValue(val_bs.value(), max_entry_chunks, max_entry_size_in_bytes, max_entry_size_in_rows));
        }
        catch (...)
        {
            LOG_WARNING(logger, "Skipping malformed cache entry during dump: {}", getCurrentExceptionMessage(false));
        }
    }

    return result;
}

/// Return the total number of query-cache entries in the selected Redis database.
/// Excludes lock keys ending with `:lock`.
size_t RedisRemoteCacheBackend::count()
try
{
    const std::string pattern = dataKeysPatternForAllTags();

    return execute([&](Poco::Redis::Client & client)
    {
        return executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.count << "0" << pattern;
            return static_cast<size_t>(client.execute<Poco::Int64>(cmd));
        });
    });
}
catch (...)
{
    LOG_WARNING(logger, "Failed to count external query cache entries: {}", getCurrentExceptionMessage(false));
    return 0;
}

/// Atomically acquire a lock using `SET key <token> NX PX ttl_ms`.
/// Returns the unique token on success, or an empty string on failure.
/// The token is a random UUID that identifies lock ownership, enabling
/// safe compare-and-delete in `releaseLock`.
String RedisRemoteCacheBackend::tryAcquireLock(const std::string & redis_key, std::chrono::milliseconds ttl)
try
{
    String token = toString(UUIDHelpers::generateV4());

    auto reply = execute([&](Poco::Redis::Client & client)
    {
        Poco::Redis::Command cmd("SET");
        cmd << redis_key << token << "NX" << "PX" << std::to_string(ttl.count());

        /// SET ... NX returns SimpleString "+OK" on success, or Null BulkString
        /// when the key already exists. Use `sendCommand` to handle both reply
        /// types — `execute<BulkString>` throws `BadCastException` on `SimpleString`.
        return client.sendCommand(cmd);
    });

    return reply->isSimpleString() ? token : String{};
}
catch (...)
{
    LOG_WARNING(logger, "Failed to acquire lock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return {};
}

/// Best-effort lock release via Lua compare-and-delete. Only deletes
/// the key if the stored value matches the given token, preventing a
/// slow writer from accidentally deleting a lock re-acquired by
/// another node after TTL expiry. Never throws.
void RedisRemoteCacheBackend::releaseLock(const std::string & redis_key, const String & token)
try
{
    execute([&](Poco::Redis::Client & client)
    {
        executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.release_lock << "1" << redis_key << token;
            client.execute<Poco::Int64>(cmd);
        });
    });
}
catch (...)
{
    LOG_WARNING(logger, "Failed to release lock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
}

bool RedisRemoteCacheBackend::renewLock(const std::string & redis_key, const String & token, std::chrono::milliseconds ttl)
try
{
    return execute([&](Poco::Redis::Client & client)
    {
        return executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.renew_lock << "1" << redis_key << token << std::to_string(ttl.count());
            return client.execute<Poco::Int64>(cmd) != 0;
        });
    });
}
catch (...)
{
    LOG_WARNING(logger, "Failed to renew lock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return false;
}

/// Check lock existence via `EXISTS`. Returns false on error.
bool RedisRemoteCacheBackend::lockExists(const std::string & redis_key)
try
{
    return execute([&](Poco::Redis::Client & client)
    {
        Poco::Redis::Command cmd("EXISTS");
        cmd << redis_key;
        return client.execute<Poco::Int64>(cmd) > 0;
    });
}
catch (...)
{
    LOG_WARNING(logger, "Failed to check lock existence for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return false;
}

/// Atomically check for an existing entry and try to acquire the stampede
/// lock if the entry is missing. Uses the `GET_OR_LOCK_SCRIPT` Lua script
/// to combine GET + SET NX into a single RTT.
RedisRemoteCacheBackend::GetOrLockResult RedisRemoteCacheBackend::getOrTryAcquireLock(
    const String & redis_key,
    const std::string & lock_key,
    const String & token,
    std::chrono::milliseconds lock_ttl)
try
{
    const std::string ttl_ms_str = std::to_string(lock_ttl.count());

    auto reply = execute([&](Poco::Redis::Client & client)
    {
        return executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.get_or_lock << "2" << redis_key << lock_key << token << ttl_ms_str;
            return client.execute<Poco::Redis::Array>(cmd);
        });
    });

    if (reply.size() < 2)
        return {0, {}};

    int status = static_cast<int>(reply.get<Poco::Int64>(0));
    std::string data;
    if (status == 1)
    {
        const auto & bs = reply.get<Poco::Redis::BulkString>(1);
        if (!bs.isNull())
            data = bs.value();
    }

    return {status, std::move(data)};
}
catch (...)
{
    LOG_WARNING(logger, "Failed to execute getOrTryAcquireLock for key {}: {}", redis_key, getCurrentExceptionMessage(false));
    return {0, {}};
}

QueryResultCache::WriteContext RedisRemoteCacheBackend::getWriteContext(const String & tag)
try
{
    QueryResultCache::WriteContext write_context;

    auto reply = execute([&](Poco::Redis::Client & client)
    {
        Poco::Redis::Command cmd("MGET");
        cmd << globalGenerationKey() << tagGenerationKey(tag);
        return client.execute<Poco::Redis::Array>(cmd);
    });

    if (reply.size() >= 2)
    {
        const auto & global_bs = reply.get<Poco::Redis::BulkString>(0);
        if (!global_bs.isNull())
            write_context.global_generation = static_cast<UInt64>(std::stoull(global_bs.value()));

        const auto & tag_bs = reply.get<Poco::Redis::BulkString>(1);
        if (!tag_bs.isNull())
            write_context.tag_generation = static_cast<UInt64>(std::stoull(tag_bs.value()));
    }

    return write_context;
}
catch (...)
{
    LOG_WARNING(logger, "Failed to read query cache generations: {}", getCurrentExceptionMessage(false));
    return {};
}

void RedisRemoteCacheBackend::bumpGeneration(const std::optional<String> & tag)
try
{
    execute([&](Poco::Redis::Client & client)
    {
        Poco::Redis::Command cmd("INCR");
        cmd << (tag ? tagGenerationKey(*tag) : globalGenerationKey());
        client.execute<Poco::Int64>(cmd);
    });
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to bump external query result cache generation: {}", getCurrentExceptionMessage(false));
}

void RedisRemoteCacheBackend::clearWithGenerationBump(const std::optional<String> & tag)
try
{
    const bool has_tag = tag.has_value();
    const std::string data_pattern = has_tag ? dataKeysPatternForTag(*tag) : dataKeysPatternForAllTags();
    const std::string lock_pattern = has_tag ? lockKeysPatternForTag(*tag) : lockKeysPatternForAllTags();
    const std::string generation_key = has_tag ? tagGenerationKey(*tag) : globalGenerationKey();
    /// Empty `expected_tag` disables the in-script exact-match filter for the
    /// all-tags path, where the qcache-namespaced glob is already the right scope.
    const std::string expected_tag = has_tag ? *tag : std::string();

    execute([&](Poco::Redis::Client & client)
    {
        executeWithNoscriptRetry(client, [&](const ScriptShas & shas)
        {
            Poco::Redis::Command cmd("EVALSHA");
            cmd << shas.clear_and_bump << "1" << generation_key << data_pattern << lock_pattern << "100" << expected_tag;
            client.execute<Poco::Int64>(cmd);
        });
    });
}
catch (...)
{
    throw Exception(ErrorCodes::EXTERNAL_QUERY_RESULT_CACHE_ERROR,
        "Failed to clear external query result cache and bump generation: {}", getCurrentExceptionMessage(false));
}

}
