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

    QueryContextPtr tryGetQueryContext();

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FilesystemCacheSettings & settings);

    /// Releases this holder's reference to the query context and removes the contexts of already
    /// finished queries, returning them so that the caller destroys them after releasing the lock
    /// (see ~QueryContextHolder). A context of a running query is kept even without any holder:
    /// read buffers of one query do not necessarily overlap in time, and a new context would mean
    /// a new budget for the same query.
    std::vector<QueryContextPtr> removeQueryContext(QueryContextPtr & context);

    /// Uncharge an evicted segment from every live query: it may have been cached by a query
    /// other than the one which evicted it (or by several queries at once).
    void unchargeEvictedSegment(const FileCacheKey & key, size_t offset, const CachePriorityGuard::WriteLock &);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recache_on_query_limit_exceeded; }

        /// The query this context belongs to has finished, so the context must not be handed
        /// out anymore: `query_id` is reusable and a new query must start with a fresh budget.
        bool isQueryFinished() const { return has_thread_group && thread_group.expired(); }

        /// Whether the context may be dropped once it has no holders left. Only true if the query
        /// has finished or cannot be tracked at all (background operations, unit tests).
        bool canBeDroppedWithoutHolders() const { return !has_thread_group || thread_group.expired(); }

        Priority::IteratorPtr tryGet(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::WriteLock &);

        /// Returns the iterator of the created entry, so that the caller can account
        /// the reserved size in it once the space is actually reserved (`incrementSize`).
        Priority::IteratorPtr add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const CachePriorityGuard::WriteLock &);

        /// Drops the record and its per-query queue entry, if this query has one for
        /// `key`:`offset`. A missing record is not an error: eviction candidates are
        /// collected from the whole cache, so most of them were never cached by this query.
        /// Returns whether a record was removed.
        bool tryRemove(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::WriteLock &);

        /// Give back space which was reserved but not written (reserve-ahead surplus returned
        /// by `FileSegment::shrinkFileSegmentToDownloadedSize`). No-op without a record.
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
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
    /// The single lock guarding `query_map`. It is a leaf mutex, deliberately not one of the
    /// cache locks: `getOrSetQueryContext` runs per read buffer creation and `tryGetQueryContext`
    /// per space reservation, so neither should contend on the cache priority lock.
    mutable std::mutex query_map_mutex;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
