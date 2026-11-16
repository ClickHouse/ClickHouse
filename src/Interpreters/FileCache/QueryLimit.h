#pragma once
#include <Interpreters/FileCache/Guards.h>
#include <Interpreters/FileCache/LRUFileCachePriority.h>

namespace DB
{
struct FilesystemCacheSettings;
class FileSegment;

class FileCacheQueryLimit
{
public:
    class QueryContext;
    using QueryContextPtr = std::shared_ptr<QueryContext>;

    /// `mutex` protects `query_map` and each `QueryContext`'s `records`. It is separate from
    /// any priority's `priority_guard`, so per-query bookkeeping does not serialize on those.
    using Lock = std::unique_lock<std::mutex>;
    Lock lock() { return Lock(mutex); }

    QueryContextPtr tryGetQueryContext();

    QueryContextPtr getOrSetQueryContext(
        const std::string & query_id,
        const FilesystemCacheSettings & settings,
        const Lock &);

    void removeQueryContext(const std::string & query_id, const Lock &);

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
            const Lock &);

        void add(
            KeyMetadataPtr key_metadata,
            size_t offset,
            size_t size,
            const Lock &);

        void remove(
            const Key & key,
            size_t offset,
            const Lock &);

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
    std::mutex mutex;
    using QueryContextMap = std::unordered_map<String, QueryContextPtr>;
    QueryContextMap query_map;
};

using FileCacheQueryLimitPtr = std::unique_ptr<FileCacheQueryLimit>;

}
