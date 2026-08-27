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

bool FileCacheQueryLimit::isCurrentThreadInQuery()
{
    return isQueryInitialized();
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetCurrentQueryContextUnlocked() const
{
    if (!isQueryInitialized())
        return nullptr;

    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    if (query_iter == query_map.end() || query_iter->second->isQueryFinished())
        return nullptr;
    return query_iter->second;
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext()
{
    if (!hasQueryContexts())
        return nullptr;

    /// Destroyed after the lock is released.
    std::vector<QueryContextPtr> doomed;
    {
        std::lock_guard lock(query_map_mutex);
        if (auto context = tryGetCurrentQueryContextUnlocked())
            return context;

        /// This query is not limited, yet some context is still here. Sweep the finished ones, so
        /// that a context left behind by a query which is long gone does not keep every reservation
        /// in this cache taking this lock.
        sweepDroppableContextsUnlocked(doomed, /* keep_query_id */{}, /* only_finished_queries */true);
    }
    return nullptr;
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

        /// Drop this holder's own reference to the context under the lock, before the sweep below
        /// looks at use_count(). use_count() is not a synchronization primitive, so the decision
        /// must be made after every reference change to the context is serialized by this mutex
        /// (which also guards getOrSetQueryContext). Deciding before dropping the reference (or
        /// dropping it outside the lock) is a TOCTOU: two holders releasing at once can both observe
        /// the shared count and both skip the erase, orphaning the map entry (see #109508).
        context.reset();

        sweepDroppableContextsUnlocked(doomed, /* keep_query_id */{}, /* only_finished_queries */false);
    }
    return doomed;
}

void FileCacheQueryLimit::sweepDroppableContextsUnlocked(
    std::vector<QueryContextPtr> & doomed, const String & keep_query_id, bool only_finished_queries)
{
    for (auto it = query_map.begin(); it != query_map.end();)
    {
        /// The map entry is the only owner left and the query is gone. A context of an untracked
        /// query (no thread group) is only dropped by its own holder, which is the one thing known
        /// to end its lifetime.
        const bool droppable = only_finished_queries
            ? it->second->isQueryFinished()
            : it->second->canBeDroppedWithoutHolders();
        if (it->first != keep_query_id && it->second.use_count() == 1 && droppable)
        {
            doomed.push_back(std::move(it->second));
            it = query_map.erase(it);
        }
        else
            ++it;
    }
    live_contexts.store(query_map.size(), std::memory_order_release);
}

void FileCacheQueryLimit::unchargeRemovedSegment(const FileCacheKey & key, size_t offset)
{
    if (!hasQueryContexts())
        return;

    std::lock_guard map_lock(query_map_mutex);
    for (auto & [_, context] : query_map)
        context->unchargeRemoved(key, offset);
}

bool FileCacheQueryLimit::QueryContext::fits(size_t size) const
{
    const size_t limit = priority.getSizeLimitApprox();
    return limit == 0 || priority.getSizeApprox() + size <= limit;
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const FilesystemCacheSettings & settings)
{
    if (query_id.empty())
        return nullptr;

    /// Declared before the lock, so that dropped contexts are destroyed after it is released.
    std::vector<QueryContextPtr> doomed;
    {
        std::lock_guard lock(query_map_mutex);

        /// A query which released its last holder before finishing leaves its context behind, and
        /// only holder destruction sweeps them, which may never come again. Sweep here too, so that
        /// the next query arriving in this cache cleans up after the previous ones.
        sweepDroppableContextsUnlocked(doomed, query_id, /* only_finished_queries */true);

        auto it = query_map.find(query_id);
        /// query_id is reusable, so an entry of a finished query must not be inherited by a new one.
        if (it != query_map.end() && it->second->isQueryFinished())
        {
            doomed.push_back(std::move(it->second));
            query_map.erase(it);
            it = query_map.end();
        }

        if (it == query_map.end())
        {
            /// Constructed before it goes into the map, so that a throwing constructor cannot leave
            /// an empty entry behind.
            auto context = std::make_shared<QueryContext>(
                settings.query_limit_bytes,
                !settings.skip_download_if_exceeds_per_query_cache_write_limit);
            it = query_map.emplace(query_id, std::move(context)).first;
            live_contexts.store(query_map.size(), std::memory_order_release);
        }

        return it->second;
    }
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
    size_t size)
{
    auto it = getPriority().add(key_metadata, offset, size, /* state_lock */nullptr);

    std::lock_guard records_lock(records_mutex);
    auto [_, inserted] = records.emplace(FileCacheKeyAndOffset{key_metadata->key, offset}, it);
    if (!inserted)
    {
        it->remove();
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key_metadata->key);
    }
    return it;
}

bool FileCacheQueryLimit::QueryContext::tryRemove(const Key & key, size_t offset)
{
    std::lock_guard records_lock(records_mutex);
    auto record = records.find({key, offset});
    if (record == records.end())
        return false;

    record->second->remove();
    records.erase(record);
    return true;
}

void FileCacheQueryLimit::QueryContext::unchargeRemoved(const Key & key, size_t offset)
{
    std::lock_guard records_lock(records_mutex);
    auto record = records.find({key, offset});
    if (record == records.end())
        return;

    /// Releases the charge of the entry (sets its size to zero) without touching the queue, which
    /// is what makes this callable while a segment is being removed, without the write lock. The
    /// entry itself is taken out of the queue by `removeInvalidatedEntries`.
    record->second->invalidate();
    records.erase(record);
}

void FileCacheQueryLimit::QueryContext::removeInvalidatedEntries(size_t max_batch)
{
    getPriority().removeInvalidatedEntries(max_batch);
}

void FileCacheQueryLimit::QueryContext::tryDecrementSize(const Key & key, size_t offset, size_t size)
{
    std::lock_guard records_lock(records_mutex);
    auto record = records.find({key, offset});
    if (record == records.end())
        return;

    /// The surplus belongs to the whole file segment, while this record holds only what this query
    /// reserved of it: a segment whose download was handed over is charged to several queries.
    if (const size_t to_decrement = std::min<size_t>(size, record->second->getEntry()->size))
        record->second->decrementSize(to_decrement);
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key & key,
    size_t offset)
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
