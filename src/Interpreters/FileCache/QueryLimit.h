#pragma once
#include <Interpreters/FileCache/Guards.h>
#include <Interpreters/FileCache/LRUFileCachePriority.h>

#include <mutex>

namespace DB
{
struct FilesystemCacheSettings;
class FileSegment;
class ThreadGroup;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;
    /// A handle to a query context which does not keep it alive and, unlike a `query_id` (which is
    /// reusable), never resolves to a different query than the one it was taken from.
    using QueryContextWeakPtr = std::weak_ptr<QueryContext>;

    QueryContextPtr tryGetQueryContext();

    /// Whether any query is currently limited in this cache. Lock free, so that a cache which
    /// permits the limit costs nothing while no query actually uses it.
    bool hasQueryContexts() const { return live_contexts.load(std::memory_order_acquire) != 0; }

    size_t getQueryContextsCount() const { return live_contexts.load(std::memory_order_acquire); }

    /// Whether the calling thread belongs to a query, no matter whether that query is limited.
    static bool isCurrentThreadInQuery();

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FilesystemCacheSettings & settings);

    /// Releases this holder's reference to the query context and removes the contexts of already
    /// finished queries, returning them so that the caller destroys them after releasing the lock
    /// (see ~QueryContextHolder). A context of a running query is kept even without any holder:
    /// read buffers of one query do not necessarily overlap in time, and a new context would mean
    /// a new budget for the same query.
    std::vector<QueryContextPtr> removeQueryContext(QueryContextPtr & context);

    /// Uncharge a segment which left the cache from every live query: it may have been cached by
    /// a query other than the one removing it here (or by several queries at once).
    void unchargeRemovedSegment(const FileCacheKey & key, size_t offset);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recache_on_query_limit_exceeded; }

        /// Whether `size` more bytes fit into what this query has left. Approximate values are
        /// enough: this only decides whether to start writing more.
        bool fits(size_t size) const;

        /// The query this context belongs to has finished, so the context must not be handed
        /// out anymore: `query_id` is reusable and a new query must start with a fresh budget.
        bool isQueryFinished() const { return has_thread_group && thread_group.expired(); }

        /// Whether the context may be dropped once it has no holders left. Only true if the query
        /// has finished or cannot be tracked at all (background operations, unit tests).
        bool canBeDroppedWithoutHolders() const { return !has_thread_group || thread_group.expired(); }

        Priority::IteratorPtr tryGet(const Key & key, size_t offset);

        /// Returns the iterator of the created entry, so that the caller can account
        /// the reserved size in it once the space is actually reserved (`incrementSize`).
        Priority::IteratorPtr add(KeyMetadataPtr key_metadata, size_t offset, size_t size);

        /// Drops the record and its per-query queue entry, if this query has one for
        /// `key`:`offset`. A missing record is not an error: eviction candidates are
        /// collected from the whole cache, so most of them were never cached by this query.
        /// Returns whether a record was removed.
        bool tryRemove(const Key & key, size_t offset);

        /// The segment is gone from the cache: release its charge and drop the record. Only the
        /// charge is released here, while the queue entry is merely invalidated: taking it out of
        /// the queue needs the priority's own guard, which no removal path may block on.
        void unchargeRemoved(const Key & key, size_t offset);

        /// Takes the entries invalidated by `unchargeRemoved` out of the per-query queue, so that
        /// neither the queue nor the eviction walk over it grows with every segment this query has
        /// ever cached. Lock free while there is nothing to remove, which is the common case.
        void removeInvalidatedEntries(size_t max_batch);

        /// Give back space which was reserved but not written, at most what is charged for
        /// `key`:`offset`. No-op without a record.
        void tryDecrementSize(const Key & key, size_t offset, size_t size);

    private:
        using Records = std::unordered_map<FileCacheKeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        /// `records` and `priority` mirror the part of the cache written by this query.
        /// Structural changes of `priority` need the cache priority write lock (as for the main
        /// priority), `records` is guarded by this leaf mutex, so that returning the reserve-ahead
        /// surplus needs no cache lock.
        mutable std::mutex records_mutex;
        Records records;
        LRUFileCachePriority priority;
        const bool recache_on_query_limit_exceeded;
        const std::weak_ptr<ThreadGroup> thread_group;
        const bool has_thread_group;
    };

    struct QueryContextHolder : private boost::noncopyable
    {
        QueryContextHolder(const String & query_id_, FileCacheQueryLimit * query_limit_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        FileCacheQueryLimit * query_limit{};
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;

private:
    /// Drops the contexts which have no holder left and whose query is gone, `keep_query_id`
    /// excepted. The caller holds `query_map_mutex` and destroys `doomed` after releasing it.
    void sweepDroppableContextsUnlocked(
        std::vector<QueryContextPtr> & doomed, const String & keep_query_id, bool only_finished_queries);

    /// The context of the current query, if any. The caller holds `query_map_mutex`.
    QueryContextPtr tryGetCurrentQueryContextUnlocked() const;

    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
    /// Size of `query_map`, kept in sync with it under `query_map_mutex` to be readable without it.
    std::atomic<size_t> live_contexts = 0;
    /// The single lock guarding `query_map`. It is a leaf mutex, deliberately not one of the
    /// cache locks: `getOrSetQueryContext` runs per read buffer creation and `tryGetQueryContext`
    /// per space reservation, so neither should contend on the cache priority lock.
    mutable std::mutex query_map_mutex;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
