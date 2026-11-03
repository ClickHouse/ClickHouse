#pragma once
#include <Interpreters/FileCache/Guards.h>
#include <Interpreters/FileCache/LRUFileCachePriority.h>

#include <mutex>

namespace DB
{
struct ReadSettings;
struct FilesystemCacheSettings;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    QueryContextPtr tryGetQueryContext(const CacheStateGuard::Lock & lock);

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FilesystemCacheSettings & settings,
        const CachePriorityGuard::WriteLock &);

    /// Releases this holder's reference to the query context and, when it was the last holder,
    /// removes the map entry and returns the now-orphaned context so the caller can destroy it
    /// after releasing the cache write lock (see ~QueryContextHolder). Returns nullptr when the
    /// context is still owned by another live holder.
    QueryContextPtr removeQueryContext(const std::string & query_id, QueryContextPtr & context, const CachePriorityGuard::WriteLock &);

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
            const CachePriorityGuard::WriteLock &);

        void add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const CachePriorityGuard::WriteLock &);

        void remove(
            const Key & key,
            size_t offset,
            const CachePriorityGuard::WriteLock &);

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
        FileCache * cache{};
        FileCacheQueryLimit * query_limit{};
        QueryContextPtr context;
    };
    using QueryContextHolderPtr = std::unique_ptr<QueryContextHolder>;

private:
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
    /// query_map is reached under two different cache locks: reads (tryGetQueryContext) run under
    /// CacheStateGuard while writes (getOrSetQueryContext/removeQueryContext) run under
    /// CachePriorityGuard, so neither cache lock serializes access to the map by itself. This
    /// dedicated leaf mutex is the single lock that actually guards query_map.
    mutable std::mutex query_map_mutex;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
