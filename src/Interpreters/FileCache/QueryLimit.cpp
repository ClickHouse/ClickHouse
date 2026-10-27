#include <Interpreters/FileCache/FileCache.h>
#include <Interpreters/FileCache/Metadata.h>
#include <Interpreters/FileCache/QueryLimit.h>
#include <IO/ReadSettings.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool isQueryInitialized()
{
    return CurrentThread::isInitialized()
        && CurrentThread::get().tryGetQueryContext()
        && !CurrentThread::getQueryId().empty();
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext(const CacheStateGuard::Lock &)
{
    if (!isQueryInitialized())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

FileCacheQueryLimit::QueryContextPtr
FileCacheQueryLimit::removeQueryContext(const std::string & query_id, QueryContextPtr & context, const CachePriorityGuard::WriteLock &)
{
    QueryContextPtr doomed;
    {
        std::lock_guard lock(query_map_mutex);

        auto query_iter = query_map.find(query_id);
        const bool owns_map_entry = query_iter != query_map.end() && query_iter->second == context;

        /// Drop this holder's own reference to the context under the lock, then decide. use_count()
        /// is not a synchronization primitive, so the decision must be made after every reference
        /// change to the context is serialized by this mutex (which also guards getOrSetQueryContext).
        /// Deciding before dropping the reference (or dropping it outside the lock) is a TOCTOU:
        /// two holders releasing at once can both observe the shared count and both skip the erase,
        /// orphaning the map entry, or one can erase while the other is being revived (see #109508).
        context.reset();

        if (owns_map_entry && query_iter->second.use_count() == 1)
        {
            /// The reference this holder held is gone and the map entry is now the sole owner, so
            /// this was the last holder. Extract the pointer instead of erasing in place so the
            /// QueryContext (its records map and per-query priority queue) is destroyed by the
            /// caller after the cache write lock is released, not under it. Otherwise a query that
            /// touched many segments frees all of that state while holding cache->lockCache(),
            /// blocking unrelated reserve/eviction work for the duration of teardown.
            doomed = std::move(query_iter->second);
            query_map.erase(query_iter);
        }
        /// If owns_map_entry is false, the entry was already removed or re-created for a newer holder
        /// via getOrSetQueryContext; another live holder now owns it, so leave it in place. If the
        /// map entry is not the sole owner, another holder for the same query_id is still alive and
        /// the context must stay so the per-query limit keeps being enforced.
    }
    return doomed;
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const FilesystemCacheSettings & settings,
    const CachePriorityGuard::WriteLock &)
{
    if (query_id.empty())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            settings.max_download_size_per_query,
            !settings.skip_download_if_exceeds_per_query_cache_write_limit);
    }

    return it->second;
}

FileCacheQueryLimit::QueryContext::QueryContext(
    size_t query_cache_size,
    bool recache_on_query_limit_exceeded_)
    : priority(LRUFileCachePriority(IFileCachePriority::QueueType::Query, query_cache_size, 0))
    , recache_on_query_limit_exceeded(recache_on_query_limit_exceeded_)
{
}

void FileCacheQueryLimit::QueryContext::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock)
{
    auto it = getPriority().add(key_metadata, offset, size, lock, /* state_lock */nullptr);
    auto [_, inserted] = records.emplace(FileCacheKeyAndOffset{key_metadata->key, offset}, it);
    if (!inserted)
    {
        it->remove(lock);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key_metadata->key);
    }
}

void FileCacheQueryLimit::QueryContext::remove(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock & lock)
{
    auto record = records.find({key, offset});
    if (record == records.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no {}:{} in query context", key, offset);

    record->second->remove(lock);
    records.erase({key, offset});
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock &)
{
    auto it = records.find({key, offset});
    if (it == records.end())
        return nullptr;
    return it->second;

}

FileCacheQueryLimit::QueryContextHolder::QueryContextHolder(
    const String & query_id_,
    FileCache * cache_,
    FileCacheQueryLimit * query_limit_,
    FileCacheQueryLimit::QueryContextPtr context_)
    : query_id(query_id_)
    , cache(cache_)
    , query_limit(query_limit_)
    , context(context_)
{
}

FileCacheQueryLimit::QueryContextHolder::~QueryContextHolder()
{
    /// The last-holder decision (and the drop of this holder's reference) must happen inside
    /// removeQueryContext under the cache write lock, not here: dropping the reference or deciding
    /// outside the lock races with revival via getOrSetQueryContext and can leak or orphan the entry.
    /// context is only set when the per-query download limit is enabled, so this is a no-op otherwise.
    if (context)
    {
        /// When this is the last holder, removeQueryContext hands the context back so it is destroyed
        /// here, after the cache lock scope has ended, rather than under cache->lockCache().
        QueryContextPtr doomed;
        {
            auto lock = cache->lockCache();
            doomed = query_limit->removeQueryContext(query_id, context, lock);
        }
    }
}

}
