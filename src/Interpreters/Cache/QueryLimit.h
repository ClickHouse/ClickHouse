#pragma once
#include <Interpreters/Cache/Guards.h>
#include <Interpreters/Cache/LockedFileCachePriority.h>
#include <Interpreters/Cache/LRUFileCachePriority.h>

namespace DB
{
struct ReadSettings;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;
    class LockedQueryContext;
    using LockedQueryContextPtr = std::unique_ptr<LockedQueryContext>;

    LockedQueryContextPtr tryGetQueryContext(const CacheGuard::Lock & lock);

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id, const ReadSettings & settings, const CacheGuard::Lock &);

    void removeQueryContext(const std::string & query_id, const CacheGuard::Lock &);

private:
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;

public:
    class QueryContext
    {
    public:
        QueryContext(size_t query_cache_size, bool recache_on_query_limit_exceeded_)
            : priority(std::make_unique<LRUFileCachePriority>(query_cache_size, 0))
            , recache_on_query_limit_exceeded(recache_on_query_limit_exceeded_) {}

    private:
        friend class FileCacheQueryLimit::LockedQueryContext;

        using Records = std::unordered_map<FileCacheKeyAndOffset, IFileCachePriority::Iterator, FileCacheKeyAndOffsetHash>;
        Records records;
        FileCachePriorityPtr priority;
        const bool recache_on_query_limit_exceeded;
    };

    /// CacheGuard::Lock protects all priority queues.
    class LockedQueryContext
    {
    public:
        LockedQueryContext(QueryContextPtr context_, const CacheGuard::Lock & lock_)
            : context(context_), lock(lock_), priority(lock_, *context->priority) {}

        IFileCachePriority & getPriority() { return *context->priority; }
        const IFileCachePriority & getPriority() const { return *context->priority; }

        size_t getSize() const { return priority.getSize(); }

        size_t getSizeLimit() const { return priority.getSizeLimit(); }

        bool recacheOnFileCacheQueryLimitExceeded() const { return context->recache_on_query_limit_exceeded; }

        IFileCachePriority::Iterator tryGet(const FileCacheKey & key, size_t offset);

        void add(const FileCacheKey & key, size_t offset, IFileCachePriority::Iterator iterator);

        void remove(const FileCacheKey & key, size_t offset);

    private:
        QueryContextPtr context;
        const CacheGuard::Lock & lock;
        LockedCachePriority priority;
    };
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
