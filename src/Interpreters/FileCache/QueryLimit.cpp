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

void FileCacheQueryLimit::removeQueryContext(const std::string & query_id, const QueryContextPtr & context, const CachePriorityGuard::WriteLock &)
{
    std::lock_guard lock(query_map_mutex);
    auto query_iter = query_map.find(query_id);
    if (query_iter == query_map.end() || query_iter->second != context)
    {
        /// The entry was already removed, or was re-created for a newer holder via
        /// getOrSetQueryContext after this holder decided to release. Both writers run
        /// under the same cache write lock, so observing a different (or missing) context
        /// here means another live holder now owns it. Leave it in place.
        return;
    }

    /// Authoritative last-holder check, made atomically with the erase under the cache write
    /// lock (which also serializes getOrSetQueryContext). The two expected references are this
    /// holder's own context member and the query_map entry; more than that means another holder
    /// for the same query_id is still alive, so the context must stay.
    if (context.use_count() > 2)
        return;

    query_map.erase(query_iter);
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
    /// The last-holder decision must be made inside removeQueryContext under the cache write lock,
    /// not here: deciding before the lock races with revival via getOrSetQueryContext.
    /// context is only set when the per-query download limit is enabled, so this is a no-op otherwise.
    if (context)
    {
        auto lock = cache->lockCache();
        query_limit->removeQueryContext(query_id, context, lock);
    }
}

}
