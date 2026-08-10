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

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext()
{
    if (!isQueryInitialized())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    if (query_iter == query_map.end() || query_iter->second->isQueryFinished())
        return nullptr;
    return query_iter->second;
}

std::vector<FileCacheQueryLimit::QueryContextPtr>
FileCacheQueryLimit::removeQueryContext(QueryContextPtr & context)
{
    /// Extract the contexts instead of erasing them in place, so that they (their records maps and
    /// per-query priority queues) are destroyed by the caller after the lock is released. Otherwise
    /// a query which touched many segments frees all of that state while holding query_map_mutex,
    /// blocking unrelated queries which look up their own context on every space reservation.
    std::vector<QueryContextPtr> doomed;
    {
        std::lock_guard lock(query_map_mutex);

        /// Drop this holder's own reference to the context under the lock, before looking at
        /// use_count() below. use_count() is not a synchronization primitive, so the decision must
        /// be made after every reference change to the context is serialized by this mutex (which
        /// also guards getOrSetQueryContext). Deciding before dropping the reference (or dropping it
        /// outside the lock) is a TOCTOU: two holders releasing at once can both observe the shared
        /// count and both skip the erase, orphaning the map entry (see #109508).
        context.reset();

        /// Sweep the contexts of finished queries: this holder's own query may still be running
        /// (then its context must stay), and the last holder of an already finished query is not
        /// necessarily this one.
        for (auto it = query_map.begin(); it != query_map.end();)
        {
            if (it->second.use_count() == 1 && it->second->canBeDroppedWithoutHolders())
            {
                doomed.push_back(std::move(it->second));
                it = query_map.erase(it);
            }
            else
                ++it;
        }
    }
    return doomed;
}

void FileCacheQueryLimit::unchargeEvictedSegment(
    const FileCacheKey & key, size_t offset, const CachePriorityGuard::WriteLock & lock)
{
    std::lock_guard map_lock(query_map_mutex);
    for (auto & [_, context] : query_map)
        context->tryRemove(key, offset, lock);
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const FilesystemCacheSettings & settings)
{
    if (query_id.empty())
        return nullptr;

    std::lock_guard lock(query_map_mutex);
    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    /// query_id is reusable, so an entry of a finished query must not be inherited by a new one.
    if (!inserted && it->second->isQueryFinished())
    {
        it->second.reset();
        inserted = true;
    }

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
    , thread_group(CurrentThread::getGroup())
    , has_thread_group(thread_group.lock() != nullptr)
{
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const CachePriorityGuard::WriteLock & lock)
{
    auto it = getPriority().add(key_metadata, offset, size, lock, /* state_lock */nullptr);

    std::lock_guard records_lock(records_mutex);
    auto [_, inserted] = records.emplace(FileCacheKeyAndOffset{key_metadata->key, offset}, it);
    if (!inserted)
    {
        it->remove(lock);
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key_metadata->key);
    }
    return it;
}

bool FileCacheQueryLimit::QueryContext::tryRemove(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock & lock)
{
    std::lock_guard records_lock(records_mutex);
    auto record = records.find({key, offset});
    if (record == records.end())
        return false;

    record->second->remove(lock);
    records.erase(record);
    return true;
}

void FileCacheQueryLimit::QueryContext::tryDecrementSize(const Key & key, size_t offset, size_t size)
{
    std::lock_guard records_lock(records_mutex);
    auto record = records.find({key, offset});
    if (record != records.end())
        record->second->decrementSize(size);
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key & key,
    size_t offset,
    const CachePriorityGuard::WriteLock &)
{
    std::lock_guard records_lock(records_mutex);
    auto it = records.find({key, offset});
    if (it == records.end())
        return nullptr;
    return it->second;

}

FileCacheQueryLimit::QueryContextHolder::QueryContextHolder(
    const String & query_id_,
    FileCacheQueryLimit * query_limit_,
    FileCacheQueryLimit::QueryContextPtr context_)
    : query_id(query_id_)
    , query_limit(query_limit_)
    , context(context_)
{
}

FileCacheQueryLimit::QueryContextHolder::~QueryContextHolder()
{
    /// The drop of this holder's reference must happen inside removeQueryContext under
    /// query_map_mutex, not here: dropping it outside the lock races with getOrSetQueryContext
    /// and can orphan the map entry. context is only set when the per-query download limit is
    /// enabled, so this is a no-op otherwise.
    if (context)
    {
        /// The contexts of finished queries are handed back, so they are destroyed here, after
        /// the lock scope has ended.
        std::vector<QueryContextPtr> doomed = query_limit->removeQueryContext(context);
    }
}

}
