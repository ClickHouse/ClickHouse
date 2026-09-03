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

    /// `getOrSetQueryContext` inserts into `query_map` under `mutex`, so this lookup
    /// must take `mutex` as well.
    Lock guard(mutex);
    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

void FileCacheQueryLimit::removeQueryContext(const std::string & query_id, const Lock &)
{
    if (query_map.erase(query_id) == 0)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Attempt to release query context that does not exist (query_id: {})",
            query_id);
    }
}

FileCacheQueryLimit::QueryContextPtr FileCacheQueryLimit::getOrSetQueryContext(
    const std::string & query_id,
    const FilesystemCacheSettings & settings,
    const Lock &)
{
    if (query_id.empty())
        return nullptr;

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
    const Lock &)
{
    /// The caller's `Lock` (`FileCacheQueryLimit::mutex`) protects the `records` map.
    auto it = priority.add(key_metadata, offset, size, /* state_lock */nullptr);
    auto [_, inserted] = records.emplace(FileCacheKeyAndOffset{key_metadata->key, offset}, it);
    if (!inserted)
    {
        it->remove();
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key_metadata->key);
    }
}

void FileCacheQueryLimit::QueryContext::remove(
    const Key & key,
    size_t offset,
    const Lock &)
{
    auto record = records.find({key, offset});
    if (record == records.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no {}:{} in query context", key, offset);

    record->second->remove();
    records.erase({key, offset});
}

IFileCachePriority::IteratorPtr FileCacheQueryLimit::QueryContext::tryGet(
    const Key & key,
    size_t offset,
    const Lock &)
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
    if (!context)
        return;

    /// Last-holder release: erase the `query_map` entry. Reading `use_count` under `query_limit->lock()`
    /// stops a concurrent `tryGetQueryContext` from handing out a new reference between the check and
    /// the erase (which would split per-query accounting); moving our reference into a local first stops
    /// two concurrent last holders from each seeing the other's reference and both skipping the erase (a leak).
    auto lock = query_limit->lock();
    auto context_to_release = std::move(context);
    /// `use_count` of 2 means the only references left are this local and the `query_map` entry.
    if (context_to_release.use_count() == 2)
        query_limit->removeQueryContext(query_id, lock);
}

}
