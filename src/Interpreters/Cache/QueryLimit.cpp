#include <Interpreters/Cache/QueryLimit.h>
#include <Interpreters/Cache/Metadata.h>

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

FileCacheQueryLimit::LockedQueryContextPtr FileCacheQueryLimit::tryGetQueryContext(const CacheGuard::Lock & lock)
{
    if (!isQueryInitialized())
        return nullptr;

    auto query_iter = query_map.find(std::string(CurrentThread::getQueryId()));
    return (query_iter == query_map.end()) ? nullptr : std::make_unique<LockedQueryContext>(query_iter->second, lock);
}

void FileCacheQueryLimit::removeQueryContext(const std::string & query_id, const CacheGuard::Lock &)
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
    const CacheGuard::Lock &)
{
    if (query_id.empty())
        return nullptr;

    auto [it, inserted] = query_map.emplace(query_id, nullptr);
    if (inserted)
    {
        it->second = std::make_shared<QueryContext>(
            settings.filesystem_cache_max_download_size, !settings.skip_download_if_exceeds_query_cache);
    }

    return it->second;
}

void FileCacheQueryLimit::LockedQueryContext::add(const FileCacheKey & key, size_t offset, IFileCachePriority::Iterator iterator)
{
    auto [_, inserted] = context->records.emplace(FileCacheKeyAndOffset{key, offset}, iterator);
    if (!inserted)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot add offset {} to query context under key {}, it already exists",
            offset, key.toString());
    }
}

void FileCacheQueryLimit::LockedQueryContext::remove(const FileCacheKey & key, size_t offset)
{
    auto record = context->records.find({key, offset});
    if (record == context->records.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no {}:{} in query context", key.toString(), offset);

    LockedCachePriorityIterator(lock, record->second).remove();
    context->records.erase({key, offset});
}

IFileCachePriority::Iterator FileCacheQueryLimit::LockedQueryContext::tryGet(const FileCacheKey & key, size_t offset)
{
    auto it = context->records.find({key, offset});
    if (it == context->records.end())
        return nullptr;
    return it->second;

}

}
