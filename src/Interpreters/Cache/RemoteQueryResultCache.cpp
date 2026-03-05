#include <Interpreters/Cache/RemoteQueryResultCache.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <thread>

namespace DB
{

RemoteQueryResultCache::RemoteQueryResultCache(
    RedisConfiguration redis_config,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    size_t max_entries_for_dump_,
    std::chrono::milliseconds lock_ttl_,
    std::chrono::milliseconds lock_poll_interval_,
    std::chrono::milliseconds lock_max_wait_)
    : backend(std::move(redis_config))
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , max_entries_for_dump(max_entries_for_dump_)
    , lock_ttl(lock_ttl_)
    , lock_poll_interval(lock_poll_interval_)
    , lock_max_wait(lock_max_wait_)
{
}

/// Fetch the cached result from Redis, verify access rights and
/// staleness, and wrap it in a reader. Returns an empty reader on
/// miss or access/staleness failure.
QueryResultCacheReader RemoteQueryResultCache::createReader(const Key & key)
{
    auto result = backend.getWithKey(key);

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
    return QueryResultCacheWriter(*this, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

/// Build the lock key by appending `:lock` to the Redis data key.
std::string RemoteQueryResultCache::lockKey(const std::string & redis_key)
{
    return redis_key + ":lock";
}

/// Poll Redis until a valid entry appears, the lock disappears
/// (holder crashed), or timeout is reached.
bool RemoteQueryResultCache::pollForResult(const Key & key, const std::string & redis_key)
{
    const auto deadline = std::chrono::steady_clock::now() + lock_max_wait;
    const std::string lk = lockKey(redis_key);

    while (std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(lock_poll_interval);

        auto result = backend.getWithKey(key);
        if (result.has_value() && !QueryResultCache::IsStale()(result->first))
        {
            LOG_TRACE(logger, "Polled and found a valid cache entry for query {} (remote cache)", doubleQuoteString(key.query_string));
            return true;
        }

        /// If the lock is gone (holder crashed or TTL expired),
        /// stop waiting and let this node execute the query itself.
        if (!backend.lockExists(lk))
        {
            LOG_TRACE(logger, "Lock for query {} disappeared before result was ready, degrading to execution", doubleQuoteString(key.query_string));
            return false;
        }
    }

    LOG_TRACE(logger, "Timed out waiting for cache result for query {}, degrading to execution", doubleQuoteString(key.query_string));
    return false;
}

/// Anti-stampede protocol: if no result exists, try to acquire an
/// `IN_PROGRESS` lock so only one node computes the result while
/// others poll and wait.
bool RemoteQueryResultCache::hasNonStaleEntry(const Key & key)
{
    const std::string redis_key = key.encodeToRedisKey();

    /// Check if a real result already exists.
    auto result = backend.getWithKey(key);
    if (result.has_value() && !QueryResultCache::IsStale()(result->first))
        return true;

    /// Check if this node already holds the lock for this key
    /// (re-entrant call from `finalizeWrite`). If so, let the
    /// writer proceed to write.
    {
        std::lock_guard lock(mutex);
        if (held_locks.count(redis_key))
            return false;
    }

    /// Try to acquire the lock atomically (`SET NX PX`).
    if (backend.tryAcquireLock(lockKey(redis_key), lock_ttl))
    {
        std::lock_guard lock(mutex);
        held_locks.insert(redis_key);
        LOG_TRACE(logger, "Acquired IN_PROGRESS lock for query {} (remote cache)", doubleQuoteString(key.query_string));
        return false; /// This node becomes the writer.
    }

    /// Another node holds the lock — poll for the result.
    LOG_TRACE(logger, "Another node is computing the result for query {}, waiting... (remote cache)", doubleQuoteString(key.query_string));
    return pollForResult(key, redis_key);
}

/// Write the cache entry to Redis with a TTL derived from the key's
/// `expires_at` timestamp, then release the `IN_PROGRESS` lock.
void RemoteQueryResultCache::setEntry(const Key & key, std::shared_ptr<Entry> entry)
{
    if (!entry)
        return;

    /// Compute remaining TTL. Skip write if the entry already expired.
    const auto now = std::chrono::system_clock::now();
    if (key.expires_at <= now)
    {
        LOG_TRACE(logger, "Skipped remote write because the entry has already expired, query: {}", doubleQuoteString(key.query_string));
        return;
    }

    const auto ttl = std::chrono::duration_cast<std::chrono::milliseconds>(key.expires_at - now);

    backend.set(key, *entry, ttl);

    /// Release the `IN_PROGRESS` lock now that the real result is in
    /// Redis.
    const std::string redis_key = key.encodeToRedisKey();
    backend.releaseLock(lockKey(redis_key));
    {
        std::lock_guard lock(mutex);
        held_locks.erase(redis_key);
    }
}

/// Release the `IN_PROGRESS` lock when the writer decides not to
/// store the result (e.g. query too fast, result too large).
void RemoteQueryResultCache::cancelWrite(const Key & key)
{
    const std::string redis_key = key.encodeToRedisKey();
    backend.releaseLock(lockKey(redis_key));
    std::lock_guard lock(mutex);
    held_locks.erase(redis_key);
}

/// Clear all entries or entries matching a tag prefix, and reset
/// local tracking state.
void RemoteQueryResultCache::clear(const std::optional<String> & tag)
{
    if (tag)
        backend.clearByTag(*tag);
    else
        backend.clear();

    std::lock_guard lock(mutex);
    times_executed.clear();
    held_locks.clear();
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
    auto pairs = backend.dump(max_entries_for_dump);

    std::vector<QueryResultCache::Cache::KeyMapped> result;
    result.reserve(pairs.size());
    for (auto & [k, v] : pairs)
    {
        result.push_back({std::move(k), std::make_shared<Entry>(std::move(v))});
    }
    return result;
}

}
