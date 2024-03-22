#pragma once
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>

namespace DB
{
struct ReadSettings;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    QueryContextPtr tryGetQueryContext(const CacheGuard::Lock & lock);

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const ReadSettings & settings,
        const CacheGuard::Lock &);

    void removeQueryContext(const std::string & query_id, const CacheGuard::Lock &);

    class QueryContext
    {
    public:
        using Key = FileCacheKey;
        using Priority = IFileCachePriority;
        using PriorityIterator = IFileCachePriority::Iterator;

        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_);

        Priority & getPriority() { return priority; }
        const Priority & getPriority() const { return priority; }

        bool recacheOnFileCacheQueryLimitExceeded() const { return recache_on_query_limit_exceeded; }

        IFileCachePriority::Iterator tryGet(
            const Key & key,
            size_t offset,
            const CacheGuard::Lock &);

        void add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const CacheGuard::Lock &);

        void remove(
            const Key & key,
            size_t offset,
            const CacheGuard::Lock &);

    private:
        using Records = std::unordered_map<FileCacheKeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
        Records records;
        LRUFileCachePriority priority;
        const bool recache_on_query_limit_exceeded;
    };

private:
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
