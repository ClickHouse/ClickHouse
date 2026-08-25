#pragma once
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>

#include <mutex>

namespace DB
{
struct ReadSettings;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    QueryContextPtr tryGetQueryContext(const CachePriorityGuard::Lock & lock);

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const ReadSettings & settings,
        const CachePriorityGuard::Lock &);

    /// Releases this holder's reference to the query context and, when it was the last holder,
    /// removes the map entry and returns the now-orphaned context so the caller can destroy it
    /// after releasing the cache write lock (see ~QueryContextHolder). Returns nullptr when the
    /// context is still owned by another live holder.
    QueryContextPtr removeQueryContext(const std::string & query_id, QueryContextPtr & context, const CachePriorityGuard::Lock &);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recache_on_query_limit_exceeded; }

        Priority::IteratorPtr tryGet(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::Lock &);

        void add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const FileCacheUserInfo & user,
            const CachePriorityGuard::Lock &);

        void remove(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::Lock &);

    private:
        using Records = std::unordered_map<FileCacheKeyAndOffset, Priority::IteratorPtr, FileCacheKeyAndOffsetHash>;
        Records records;
        LRUFileCachePriority priority;
        const bool recache_on_query_limit_exceeded;
    };

    struct QueryContextHolder : private boost::noncopyable
    {
        QueryContextHolder(const String & query_id_, FileCache * cache_, FileCacheQueryLimit * query_limit_, QueryContextPtr context_);

        QueryContextHolder() = default;

        ~QueryContextHolder();

        String query_id;
        FileCache * cache;
        FileCacheQueryLimit * query_limit;
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;

private:
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
    /// query_map is protected by this dedicated leaf mutex. Its callers can hold the cache
    /// priority lock, but the mutex is the single lock that serializes map access.
    mutable std::mutex query_map_mutex;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
