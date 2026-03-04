#pragma once

#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/RedisRemoteCacheBackend.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

/// Redis-backed implementation of IQueryResultCache.
///
/// - Reads and writes are delegated to RedisRemoteCacheBackend.
/// - `sizeInBytes` always returns 0 — the data lives in Redis, not in local memory.
/// - `count` delegates to Redis DBSIZE.
/// - `recordQueryRun` uses a local in-process counter (same strategy as LocalQueryResultCache).
/// - `dump` calls the backend SCAN-based dump and converts to Cache::KeyMapped for compatibility
///   with system.query_cache.
class RemoteQueryResultCache
    : public IQueryResultCache
    , private IQueryResultCacheStorage
{
public:
    RemoteQueryResultCache(
        RedisConfiguration redis_config,
        size_t max_entry_size_in_bytes_,
        size_t max_entry_size_in_rows_,
        size_t max_entries_for_dump_);

    QueryResultCacheReader createReader(const Key & key) override;

    QueryResultCacheWriter createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota) override;

    void clear(const std::optional<String> & tag) override;

    size_t sizeInBytes() const override;
    size_t count() const override;

    size_t recordQueryRun(const Key & key) override;

    std::vector<QueryResultCache::Cache::KeyMapped> dump() const override;

private:
    /// IQueryResultCacheStorage interface (used by QueryResultCacheWriter)
    void setEntry(const Key & key, std::shared_ptr<Entry> entry) override;
    bool hasNonStaleEntry(const Key & key) override;

    mutable RedisRemoteCacheBackend backend;

    /// Maximum allowed sizes for a single cache entry (enforced in setEntry).
    size_t max_entry_size_in_bytes;
    size_t max_entry_size_in_rows;

    /// Upper bound on how many entries dump() fetches from Redis.
    size_t max_entries_for_dump;

    mutable std::mutex mutex;

    /// Per-query execution counter. Used to implement query_cache_min_query_runs.
    /// Node-local: each ClickHouse node tracks its own counts independently.
    using TimesExecuted = std::unordered_map<Key, size_t, QueryResultCache::KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    LoggerPtr logger = getLogger("RemoteQueryResultCache");
};

}
