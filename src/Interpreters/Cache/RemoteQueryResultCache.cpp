#include <Interpreters/Cache/RemoteQueryResultCache.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{

RemoteQueryResultCache::RemoteQueryResultCache(
    RedisConfiguration redis_config,
    size_t max_entry_size_in_bytes_,
    size_t max_entry_size_in_rows_,
    size_t max_entries_for_dump_)
    : backend(std::move(redis_config))
    , max_entry_size_in_bytes(max_entry_size_in_bytes_)
    , max_entry_size_in_rows(max_entry_size_in_rows_)
    , max_entries_for_dump(max_entries_for_dump_)
{
}

// ---------------------------------------------------------------------------
// createReader
// ---------------------------------------------------------------------------

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

    /// Staleness check — Redis TTL guarantees the key is not expired, but the stored
    /// expires_at field is used here for consistency with the local cache path.
    if (QueryResultCache::IsStale()(stored_key))
    {
        LOG_TRACE(logger, "Stale query result found for query {} (remote cache)", doubleQuoteString(key.query_string));
        return QueryResultCacheReader(key, nullptr);
    }

    return QueryResultCacheReader(stored_key, std::make_shared<Entry>(std::move(entry)));
}

// ---------------------------------------------------------------------------
// createWriter
// ---------------------------------------------------------------------------

QueryResultCacheWriter RemoteQueryResultCache::createWriter(
    const Key & key,
    std::chrono::milliseconds min_query_runtime,
    bool squash_partial_results,
    size_t max_block_size,
    size_t /*max_query_result_cache_size_in_bytes_quota*/,
    size_t /*max_query_result_cache_entries_quota*/)
{
    /// Per-user quotas are not tracked in the remote cache (Redis manages its own TTL/eviction).
    /// Note: no mutex held here — max_entry_size_in_bytes/rows are immutable after construction.
    return QueryResultCacheWriter(*this, key, max_entry_size_in_bytes, max_entry_size_in_rows, min_query_runtime, squash_partial_results, max_block_size);
}

// ---------------------------------------------------------------------------
// IQueryResultCacheStorage: setEntry
// ---------------------------------------------------------------------------

void RemoteQueryResultCache::setEntry(const Key & key, std::shared_ptr<Entry> entry)
{
    if (!entry)
        return;

    /// Compute remaining TTL. If the entry has already expired, do not write it.
    const auto now = std::chrono::system_clock::now();
    if (key.expires_at <= now)
    {
        LOG_TRACE(logger, "Skipped remote write because the entry has already expired, query: {}", doubleQuoteString(key.query_string));
        return;
    }

    const auto ttl = std::chrono::duration_cast<std::chrono::milliseconds>(key.expires_at - now);

    backend.set(key, *entry, ttl);
}

// ---------------------------------------------------------------------------
// IQueryResultCacheStorage: hasNonStaleEntry
// ---------------------------------------------------------------------------

bool RemoteQueryResultCache::hasNonStaleEntry(const Key & key)
{
    /// Redis TTL ensures keys do not outlive their expiry, but we also check our own
    /// IsStale predicate for consistency.
    auto result = backend.getWithKey(key);
    return result.has_value() && !QueryResultCache::IsStale()(result->first);
}

// ---------------------------------------------------------------------------
// clear
// ---------------------------------------------------------------------------

void RemoteQueryResultCache::clear(const std::optional<String> & tag)
{
    if (tag)
        backend.clearByTag(*tag);
    else
        backend.clear();

    std::lock_guard lock(mutex);
    times_executed.clear();
}

// ---------------------------------------------------------------------------
// sizeInBytes / count
// ---------------------------------------------------------------------------

size_t RemoteQueryResultCache::sizeInBytes() const
{
    return 0; /// Data lives in Redis, not in local memory.
}

size_t RemoteQueryResultCache::count() const
{
    return backend.count();
}

// ---------------------------------------------------------------------------
// recordQueryRun
// ---------------------------------------------------------------------------

size_t RemoteQueryResultCache::recordQueryRun(const Key & key)
{
    std::lock_guard lock(mutex);
    size_t times = ++times_executed[key];
    /// Protect against unbounded growth (DoS via unique queries).
    static constexpr size_t TIMES_EXECUTED_MAX_SIZE = 10'000;
    if (times_executed.size() > TIMES_EXECUTED_MAX_SIZE)
        times_executed.clear();
    return times;
}

// ---------------------------------------------------------------------------
// dump
// ---------------------------------------------------------------------------

std::vector<QueryResultCache::Cache::KeyMapped> RemoteQueryResultCache::dump() const
{
    auto pairs = backend.dump(max_entries_for_dump);

    std::vector<QueryResultCache::Cache::KeyMapped> result;
    result.reserve(pairs.size());
    for (auto & [k, v] : pairs)
    {
        QueryResultCache::Cache::KeyMapped km;
        km.key = std::move(k);
        km.mapped = std::make_shared<Entry>(std::move(v));
        result.push_back(std::move(km));
    }
    return result;
}

}
