#pragma once

#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/Cache/RedisRemoteCacheBackend.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace DB
{

/// Redis-backed implementation of IQueryResultCache.
///
/// - Reads and writes are delegated to RedisRemoteCacheBackend.
/// - `sizeInBytes` always returns 0 — the data lives in Redis, not in local memory.
/// - `count` delegates to Redis SCAN-based counting of query-cache-owned keys.
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
        size_t max_entry_chunks_,
        std::chrono::milliseconds lock_ttl_,
        std::chrono::milliseconds lock_poll_interval_,
        std::chrono::milliseconds lock_max_wait_);

    ~RemoteQueryResultCache() override;

    QueryResultCacheReader createReader(const Key & key) override;

    QueryResultCacheWriter createWriter(
        const Key & key,
        std::chrono::milliseconds min_query_runtime,
        bool squash_partial_results,
        size_t max_block_size,
        size_t max_query_result_cache_size_in_bytes_quota,
        size_t max_query_result_cache_entries_quota) override;

    void clear(const std::optional<String> & tag) override;

    size_t maxSizeInBytes() const override;
    size_t sizeInBytes() const override;
    size_t count() const override;

    size_t recordQueryRun(const Key & key) override;

    std::vector<QueryResultCache::Cache::KeyMapped> dump() const override;

    void updateConfiguration(size_t max_size_in_bytes, size_t max_entries,
                             size_t max_entry_size_in_bytes_, size_t max_entry_size_in_rows_) override;

private:
    /// IQueryResultCacheStorage interface (used by QueryResultCacheWriter)
    void setEntry(const Key & key, std::shared_ptr<Entry> entry, const QueryResultCache::WriteContext & write_context) override;
    bool hasNonStaleEntry(const Key & key, const QueryResultCache::WriteContext & write_context) override;
    void cancelWrite(const Key & key, const QueryResultCache::WriteContext & write_context) override;
    QueryResultCache::WriteContext getWriteContext(const Key & key) override;

    mutable RedisRemoteCacheBackend backend;

    /// Maximum allowed sizes for a single cache entry (enforced in setEntry).
    size_t max_entry_size_in_bytes;
    size_t max_entry_size_in_rows;

    /// Lock configuration for anti-stampede protection.
    std::chrono::milliseconds lock_ttl;
    std::chrono::milliseconds lock_poll_interval;
    std::chrono::milliseconds lock_max_wait;

    mutable std::mutex mutex;

    /// Bookkeeping for an `IN_PROGRESS` lock acquired by this node. The lock is a fixed-TTL
    /// advisory lease (no renewal): the local entry — not the Redis key — is what authorizes
    /// the eventual write, and `owner_thread_id` enables the re-entrant check in
    /// `hasNonStaleEntry`. The token is passed to `releaseLock` for compare-and-delete.
    struct HeldLockInfo
    {
        String token;
        std::thread::id owner_thread_id;
    };

    /// Per-query execution counter. Used to implement query_cache_min_query_runs.
    /// Node-local: each ClickHouse node tracks its own counts independently.
    using TimesExecuted = std::unordered_map<Key, size_t, QueryResultCache::KeyHasher>;
    TimesExecuted times_executed TSA_GUARDED_BY(mutex);

    /// Redis keys of locks held by this node, mapped to their unique lock tokens.
    /// Prevents a node from polling on its own lock if hasNonStaleEntry is called
    /// again on the same key after lock acquisition. The token is passed to
    /// `releaseLock` for compare-and-delete ownership validation.
    std::unordered_map<std::string, HeldLockInfo> held_locks TSA_GUARDED_BY(mutex);

    /// Build the Redis key for the stampede lock associated with a data key.
    static std::string lockKey(const std::string & redis_key);

    /// Poll Redis until the real entry appears or the lock disappears (holder crashed) or timeout.
    /// Returns true if a valid entry was found.
    bool pollForResult(const Key & key, const std::string & redis_key);

    std::optional<HeldLockInfo> takeHeldLockInfo(const std::string & redis_key);

    LoggerPtr logger = getLogger("RemoteQueryResultCache");
};

}
