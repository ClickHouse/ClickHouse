#include <Interpreters/Cache/RemoteQueryResultCache.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Core/UUID.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <base/sleep.h>

#include <algorithm>

namespace DB
{

namespace
{

bool redisKeyHasTag(const std::string & redis_key, const String & tag)
{
    return QueryResultCacheRedisKeyUtils::hasTag(redis_key, tag);
}

}

RemoteQueryResultCache::RemoteQueryResultCache(
    RedisConfiguration redis_config,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    size_t max_entry_chunks_,
    std::chrono::milliseconds lock_ttl_,
    std::chrono::milliseconds lock_poll_interval_,
    std::chrono::milliseconds lock_max_wait_)
    : backend(std::move(redis_config), max_entry_chunks_, max_entry_size_in_bytes_, max_entry_size_in_rows_)
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , lock_ttl(lock_ttl_)
    , lock_poll_interval(lock_poll_interval_)
    , lock_max_wait(lock_max_wait_)
{
}

RemoteQueryResultCache::~RemoteQueryResultCache()
{
    std::lock_guard lock(mutex);
    /// No heartbeat threads to stop. Any locks still held by this node are left to expire by
    /// their TTL in Redis (same behavior as before, just without the renewal threads).
    held_locks.clear();
}

/// Fetch the cached result from Redis, verify access rights and
/// staleness, and wrap it in a reader. Returns an empty reader on
/// miss or access/staleness failure.
QueryResultCacheReader RemoteQueryResultCache::createReader(const Key & key)
{
    const auto write_context = getWriteContext(key);

    auto result = backend.getWithKey(key, key.encodeToRedisKey(write_context));

    if (!result.has_value() && !key.is_shared)
    {
        auto shared_result = backend.getWithKey(key, key.encodeToRedisKey(write_context, true));
        if (shared_result.has_value())
            result.emplace(std::move(shared_result.value()));
    }

    if (!result.has_value())
    {
        LOG_TRACE(logger, "No query result found for query {} (remote cache)", doubleQuoteString(key.query_string));
        return QueryResultCacheReader(key, nullptr);
    }

    auto & [stored_key, entry] = *result;

    /// Access check — same logic as LocalQueryResultCache.
    const bool is_same_user_id =
        ((!stored_key.user_id.has_value() && !key.user_id.has_value()) ||
         (stored_key.user_id.has_value() && key.user_id.has_value() && *stored_key.user_id == *key.user_id));
    const bool is_same_current_user_roles = (stored_key.current_user_roles == key.current_user_roles);
    if (!stored_key.is_shared && (!is_same_user_id || !is_same_current_user_roles))
    {
        LOG_TRACE(logger, "Inaccessible query result found for query {} (remote cache)", doubleQuoteString(key.query_string));
        return QueryResultCacheReader(key, nullptr);
    }

    /// Staleness check — Redis TTL guarantees the key is not expired,
    /// but the stored `expires_at` field is used here for consistency
    /// with the local cache path.
    if (QueryResultCache::IsStale()(stored_key))
    {
        LOG_TRACE(logger, "Stale query result found for query {} (remote cache)", doubleQuoteString(key.query_string));
        return QueryResultCacheReader(key, nullptr);
    }

    return QueryResultCacheReader(stored_key, std::make_shared<Entry>(std::move(entry)));
}

/// Per-user quotas are not tracked in the remote cache because Redis
/// manages its own TTL-based eviction independently.
QueryResultCacheWriter RemoteQueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t /*max_query_result_cache_size_in_bytes_quota*/,
    size_t /*max_query_result_cache_entries_quota*/)
{
    size_t current_max_entry_size_in_bytes = 0;
    size_t current_max_entry_size_in_rows = 0;
    {
        std::lock_guard lock(mutex);
        current_max_entry_size_in_bytes = max_entry_size_in_bytes;
        current_max_entry_size_in_rows = max_entry_size_in_rows;
    }

    return QueryResultCacheWriter(
        *this,
        key,
        current_max_entry_size_in_bytes,
        current_max_entry_size_in_rows,
        min_query_runtime,
        squash_partial_results,
        max_block_size);
}

/// Build the lock key by appending `:lock` to the Redis data key.
std::string RemoteQueryResultCache::lockKey(const std::string & redis_key)
{
    return redis_key + ":lock";
}

std::optional<RemoteQueryResultCache::HeldLockInfo> RemoteQueryResultCache::takeHeldLockInfo(const std::string & redis_key)
{
    std::lock_guard lock(mutex);
    auto it = held_locks.find(redis_key);
    if (it == held_locks.end())
        return std::nullopt;

    HeldLockInfo info = std::move(it->second);
    held_locks.erase(it);
    return info;
}

/// Poll Redis until a valid entry appears, the lock disappears
/// (holder crashed), or timeout is reached.
bool RemoteQueryResultCache::pollForResult(const Key & key, const std::string & redis_key)
{
    const auto deadline = std::chrono::steady_clock::now() + lock_max_wait;
    const std::string lk = lockKey(redis_key);

    while (true)
    {
        auto result = backend.getWithKey(key, redis_key);
        if (result.has_value() && !QueryResultCache::IsStale()(result->first))
        {
            LOG_TRACE(logger, "Polled and found a valid cache entry for query {} (remote cache)", doubleQuoteString(key.query_string));
            return true;
        }

        /// If the lock is gone (holder crashed or TTL expired),
        /// re-check once immediately before degrading to execution.
        /// This closes the race where the writer stores the result and
        /// releases the lock between the GET above and the EXISTS here.
        if (!backend.lockExists(lk))
        {
            auto result_after_unlock = backend.getWithKey(key, redis_key);
            if (result_after_unlock.has_value() && !QueryResultCache::IsStale()(result_after_unlock->first))
            {
                LOG_TRACE(logger, "Lock for query {} disappeared after the result was written, using the cached value (remote cache)", doubleQuoteString(key.query_string));
                return true;
            }

            LOG_TRACE(logger, "Lock for query {} disappeared before result was ready, degrading to execution", doubleQuoteString(key.query_string));
            return false;
        }

        if (std::chrono::steady_clock::now() >= deadline)
            break;

        sleepForMilliseconds(lock_poll_interval.count());
    }

    LOG_TRACE(logger, "Timed out waiting for cache result for query {}, degrading to execution", doubleQuoteString(key.query_string));
    return false;
}

/// Anti-stampede protocol: if no result exists, try to acquire an
/// `IN_PROGRESS` lock so only one node computes the result while
/// others poll and wait.
bool RemoteQueryResultCache::hasNonStaleEntry(const Key & key, const QueryResultCache::WriteContext & write_context)
{
    const std::string redis_key = key.encodeToRedisKey(write_context);

    /// Check if this node already holds the lock for this key
    /// (re-entrant call from `finalizeWrite`). If so, let the
    /// writer proceed to write.
    {
        std::lock_guard lock(mutex);
        auto it = held_locks.find(redis_key);
        if (it != held_locks.end() && it->second.owner_thread_id == std::this_thread::get_id())
            return false;
    }

    /// Generate a unique token for the potential lock acquisition.
    String token = toString(UUIDHelpers::generateV4());
    const std::string lk = lockKey(redis_key);

    /// Atomically: GET the data key, or SET NX the lock key if data is missing.
    /// This combines the previous 2-RTT (GET + SET NX) into a single RTT.
    auto gor = backend.getOrTryAcquireLock(redis_key, lk, token, lock_ttl);

    if (gor.status == 1)
    {
        /// Data found — verify it is not stale.
        try
        {
            ReadBufferFromString buf(gor.data);
            auto stored_key = QueryResultCache::Key::deserializeFrom(buf);
            if (!QueryResultCache::IsStale()(stored_key))
                return true;
        }
        catch (...)
        {
            LOG_WARNING(logger, "Failed to deserialize cached entry during hasNonStaleEntry: {}", getCurrentExceptionMessage(false));
        }

        /// Data is stale or malformed — fall through to try acquiring the lock
        /// via the original two-step path (the Lua script already returned the
        /// data without acquiring a lock).
        auto fallback_token = backend.tryAcquireLock(lk, lock_ttl);
        if (!fallback_token.empty())
        {
            std::lock_guard lock(mutex);
            held_locks.emplace(redis_key, HeldLockInfo{fallback_token, std::this_thread::get_id()});
            LOG_TRACE(logger, "Acquired IN_PROGRESS lock for query {} after stale entry (remote cache)", doubleQuoteString(key.query_string));
            return false;
        }

        LOG_TRACE(logger, "Another node is computing the result for query {}, waiting... (remote cache)", doubleQuoteString(key.query_string));
        return pollForResult(key, redis_key);
    }

    if (gor.status == 2)
    {
        /// Lock acquired — this node becomes the writer.
        std::lock_guard lock(mutex);
        held_locks.emplace(redis_key, HeldLockInfo{token, std::this_thread::get_id()});
        LOG_TRACE(logger, "Acquired IN_PROGRESS lock for query {} (remote cache)", doubleQuoteString(key.query_string));
        return false;
    }

    /// status == 0: another node holds the lock — poll for the result.
    LOG_TRACE(logger, "Another node is computing the result for query {}, waiting... (remote cache)", doubleQuoteString(key.query_string));
    return pollForResult(key, redis_key);
}

/// Write the cache entry to Redis with a TTL derived from the key's
/// `expires_at` timestamp, then release the `IN_PROGRESS` lock.
void RemoteQueryResultCache::setEntry(const Key & key, std::shared_ptr<Entry> entry, const QueryResultCache::WriteContext & write_context)
{
    if (!entry)
    {
        cancelWrite(key, write_context);
        return;
    }

    /// Compute remaining TTL. Skip write if the entry already expired.
    const auto now = std::chrono::system_clock::now();
    if (key.expires_at <= now)
    {
        LOG_TRACE(logger, "Skipped remote write because the entry has already expired, query: {}", doubleQuoteString(key.query_string));
        cancelWrite(key, write_context);
        return;
    }

    const auto ttl = std::chrono::duration_cast<std::chrono::milliseconds>(key.expires_at - now);

    const std::string redis_key = key.encodeToRedisKey(write_context);
    auto lock_info = takeHeldLockInfo(redis_key);
    if (!lock_info.has_value())
        return;

    /// The lock is a fixed-TTL advisory lease (no heartbeat). `setIfValid` no longer depends on
    /// the lock still being held, so a write that outlives the lock TTL still succeeds; the Lua
    /// script cleans up the lock only when the token still matches.
    const bool stored = backend.setIfValid(key, *entry, redis_key, ttl, write_context, lock_info->token);

    if (!stored)
        backend.releaseLock(lockKey(redis_key), lock_info->token);
}

/// Release the `IN_PROGRESS` lock when the writer decides not to
/// store the result (e.g. query too fast, result too large).
/// Only releases the lock if this node actually acquired it,
/// preventing accidental deletion of another node's lock.
void RemoteQueryResultCache::cancelWrite(const Key & key, const QueryResultCache::WriteContext & write_context)
{
    const std::string redis_key = key.encodeToRedisKey(write_context);
    auto lock_info = takeHeldLockInfo(redis_key);
    if (!lock_info.has_value())
        return;

    backend.releaseLock(lockKey(redis_key), lock_info->token);
}

QueryResultCache::WriteContext RemoteQueryResultCache::getWriteContext(const Key & key)
{
    return backend.getWriteContext(key.tag);
}

/// Clear all entries or entries matching a tag prefix, and reset
/// local tracking state.
///
/// Ordering: delete keys first, then bump the generation counter.
///
/// This prevents a read-side race where `createReader` could serve
/// a logically-invalidated entry between `bumpGeneration` (INCR)
/// and the actual key deletion (SCAN + DEL).  By deleting first,
/// any concurrent reader will see a cache miss.
///
/// A minor write-side window remains: a concurrent writer that
/// captured the old generation (via `getWriteContext`) before the
/// deletion could re-create an entry between DELETE and INCR.
/// This is acceptable because:
///   1. The entry contains freshly computed data with a valid TTL
///      and will expire naturally.
///   2. `SYSTEM CLEAR QUERY CACHE` is a rare administrative operation.
///   3. The bumped generation ensures `setIfValid` will reject any
///      subsequent write that captured the stale generation.
void RemoteQueryResultCache::clear(const std::optional<String> & tag)
{
    backend.clearWithGenerationBump(tag);

    std::vector<std::pair<std::string, HeldLockInfo>> locks_to_release;
    {
        std::lock_guard lock(mutex);
        times_executed.clear();

        if (tag)
        {
            for (auto it = held_locks.begin(); it != held_locks.end();)
            {
                if (redisKeyHasTag(it->first, *tag))
                {
                    locks_to_release.emplace_back(it->first, std::move(it->second));
                    it = held_locks.erase(it);
                }
                else
                {
                    ++it;
                }
            }
        }
        else
        {
            locks_to_release.reserve(held_locks.size());
            for (auto & [redis_key, lock_info] : held_locks)
                locks_to_release.emplace_back(redis_key, std::move(lock_info));
            held_locks.clear();
        }
    }

    for (auto & [redis_key, lock_info] : locks_to_release)
        backend.releaseLock(lockKey(redis_key), lock_info.token);
}

void RemoteQueryResultCache::updateConfiguration(
    size_t /*max_size_in_bytes*/, size_t /*max_entries*/,
    size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_)
{
    {
        std::lock_guard lock(mutex);
        max_entry_size_in_bytes = max_entry_size_in_bytes_;
        max_entry_size_in_rows = max_entry_size_in_rows_;
    }
    /// Propagate the new bounds to the Redis backend so a malformed payload still gets rejected at deserialization time.
    backend.setEntrySizeLimits(max_entry_size_in_bytes_, max_entry_size_in_rows_);
    /// max_size_in_bytes and max_entries are managed by the Redis server
    /// (e.g. via `maxmemory`), so they are intentionally ignored here.
}

size_t RemoteQueryResultCache::maxSizeInBytes() const
{
    return 0; /// Max size is managed by Redis server, not by ClickHouse.
}

size_t RemoteQueryResultCache::sizeInBytes() const
{
    return 0; /// Data lives in Redis, not in local memory.
}

size_t RemoteQueryResultCache::count() const
{
    return backend.count();
}

/// Increment the node-local execution counter for the given query
/// key. Clears the map when it grows too large to prevent unbounded
/// memory growth from unique queries.
size_t RemoteQueryResultCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    static constexpr size_t TIMES_EXECUTED_MAX_SIZE = 10'000;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

/// Fetch entries from Redis via SCAN and convert them to the
/// `KeyMapped` format expected by `system.query_cache`.
std::vector<QueryResultCache::Cache::KeyMapped> RemoteQueryResultCache::dump() const
{
    auto pairs = backend.dump(0);

    std::vector<QueryResultCache::Cache::KeyMapped> result;
    result.reserve(pairs.size());
    for (auto & [k, v] : pairs)
    {
        result.push_back({std::move(k), std::make_shared<Entry>(std::move(v))});
    }
    return result;
}

}
