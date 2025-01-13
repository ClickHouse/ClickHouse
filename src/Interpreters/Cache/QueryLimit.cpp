#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/Metadata.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <IO/ReadSettings.h>
#include <Common/CurrentThread.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static bool isQueryInitialized()
{
    return CurrentThread::isInitialized()
        && CurrentThread::get().getQueryContext()
        && !CurrentThread::getQueryId().empty();
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::tryGetQueryContext(const CachePriorityGuard::Lock &)
{
    if (!isQueryInitialized())
        return nullptr;

    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

void FileCacheQueryLimit::removeQueryContext(const std::string & query_id, const CachePriorityGuard::Lock &)
{
    auto query_iter = query_map.find(query_id);
    if (query_iter == query_map.end())
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to release query context that does not exist (query_id: {})",
            query_id);
    }
    query_map.erase(query_iter);
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const ReadSettings & settings,
    const CachePriorityGuard::Lock &)
{
    if (query_id.empty())
        return nullptr;

    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            settings.filesystem_cache_max_download_size,
            !settings.skip_download_if_exceeds_query_cache);
    }

    return it->second;
}

FileCacheQueryLimit::QueryContext::QueryContext(
    size_t query_cache_size,
    bool recache_on_query_limit_exceeded_)
    : priority(LRUFileCachePriority(query_cache_size, 0))
    , recache_on_query_limit_exceeded(recache_on_query_limit_exceeded_)
{
}

void FileCacheQueryLimit::QueryContext::add(
    KeyMetadataPtr key_metadata,
    size_t offset,
    size_t size,
    const FileCache::UserInfo & user,
    const CachePriorityGuard::Lock & lock)
{
    auto it = getPriority().add(key_metadata, offset, size, user, lock);
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
    const CachePriorityGuard::Lock & lock)
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
    const CachePriorityGuard::Lock &)
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
    /// If only the query_map and the current holder hold the context_query,
    /// the query has been completed and the query_context is released.
    if (context && context.use_count() == 2)
    {
        auto lock = cache->lockCache();
        query_limit->removeQueryContext(query_id, lock);
    }
}

}
