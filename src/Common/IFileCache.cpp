#include "IFileCache.h"

#include <Common/hex.h>
#include <Common/CurrentThread.h>
#include <Common/SipHash.h>
#include <Common/FileCacheSettings.h>
#include <IO/ReadSettings.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int REMOTE_FS_OBJECT_CACHE_ERROR;
    extern const int LOGICAL_ERROR;
}

IFileCache::IFileCache(
    const String & cache_base_path_,
    const FileCacheSettings & cache_settings_)
    : cache_base_path(cache_base_path_)
    , max_size(cache_settings_.max_size)
    , max_element_size(cache_settings_.max_elements)
    , max_file_segment_size(cache_settings_.max_file_segment_size)
    , enable_filesystem_query_cache_limit(cache_settings_.enable_filesystem_query_cache_limit)
{
}

String IFileCache::Key::toString() const
{
    return getHexUIntLowercase(key);
}

IFileCache::Key IFileCache::hash(const String & path)
{
    return Key(sipHash128(path.data(), path.size()));
}

String IFileCache::getPathInLocalCache(const Key & key, size_t offset, bool is_persistent) const
{
    auto key_str = key.toString();
    return fs::path(cache_base_path)
        / key_str.substr(0, 3)
        / key_str
        / (std::to_string(offset) + (is_persistent ? "_persistent" : ""));
}

String IFileCache::getPathInLocalCache(const Key & key) const
{
    auto key_str = key.toString();
    return fs::path(cache_base_path) / key_str.substr(0, 3) / key_str;
}

static bool isQueryInitialized()
{
    return CurrentThread::isInitialized()
        && CurrentThread::get().getQueryContext()
        && CurrentThread::getQueryId().size != 0;
}

bool IFileCache::isReadOnly()
{
    return !isQueryInitialized();
}

void IFileCache::assertInitialized() const
{
    if (!is_initialized)
        throw Exception(ErrorCodes::REMOTE_FS_OBJECT_CACHE_ERROR, "Cache not initialized");
}

IFileCache::QueryContextPtr IFileCache::getCurrentQueryContext(std::lock_guard<std::mutex> & cache_lock)
{
    if (!isQueryInitialized())
        return nullptr;

    return getQueryContext(CurrentThread::getQueryId().toString(), cache_lock);
}

IFileCache::QueryContextPtr IFileCache::getQueryContext(const String & query_id, std::lock_guard<std::mutex> & /* cache_lock */)
{
    auto query_iter = query_map.find(query_id);
    return (query_iter == query_map.end()) ? nullptr : query_iter->second;
}

void IFileCache::removeQueryContext(const String & query_id)
{
    std::lock_guard cache_lock(mutex);
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

IFileCache::QueryContextPtr IFileCache::getOrSetQueryContext(
    const String & query_id, const ReadSettings & settings, std::lock_guard<std::mutex> & cache_lock)
{
    if (query_id.empty())
        return nullptr;

    auto context = getQueryContext(query_id, cache_lock);
    if (context)
        return context;

    auto query_context = std::make_shared<QueryContext>(settings.max_query_cache_size, settings.skip_download_if_exceeds_query_cache);
    auto query_iter = query_map.emplace(query_id, query_context).first;
    return query_iter->second;
}

IFileCache::QueryContextHolder IFileCache::getQueryContextHolder(const String & query_id, const ReadSettings & settings)
{
    std::lock_guard cache_lock(mutex);

    if (!enable_filesystem_query_cache_limit || settings.max_query_cache_size == 0)
        return {};

    /// if enable_filesystem_query_cache_limit is true, and max_query_cache_size large than zero,
    /// we create context query for current query.
    auto context = getOrSetQueryContext(query_id, settings, cache_lock);
    return QueryContextHolder(query_id, this, context);
}

void IFileCache::QueryContext::remove(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    if (cache_size < size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Deleted cache size exceeds existing cache size");

    if (!skip_download_if_exceeds_query_cache)
    {
        auto record = records.find({key, offset});
        if (record != records.end())
        {
            lru_queue.remove(record->second, cache_lock);
            records.erase({key, offset});
        }
    }
    cache_size -= size;
}

void IFileCache::QueryContext::reserve(const Key & key, size_t offset, size_t size, std::lock_guard<std::mutex> & cache_lock)
{
    if (cache_size + size > max_cache_size)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Reserved cache size exceeds the remaining cache size (key: {}, offset: {})",
            key.toString(), offset);
    }

    if (!skip_download_if_exceeds_query_cache)
    {
        auto record = records.find({key, offset});
        if (record == records.end())
        {
            auto queue_iter = lru_queue.add(key, offset, 0, cache_lock);
            record = records.insert({{key, offset}, queue_iter}).first;
        }
        record->second->size += size;
    }
    cache_size += size;
}

void IFileCache::QueryContext::use(const Key & key, size_t offset, std::lock_guard<std::mutex> & cache_lock)
{
    if (skip_download_if_exceeds_query_cache)
        return;

    auto record = records.find({key, offset});
    if (record != records.end())
        lru_queue.moveToEnd(record->second, cache_lock);
}

IFileCache::QueryContextHolder::QueryContextHolder(
    const String & query_id_,
    IFileCache * cache_,
    IFileCache::QueryContextPtr context_)
    : query_id(query_id_)
    , cache(cache_)
    , context(context_)
{
}

IFileCache::QueryContextHolder::~QueryContextHolder()
{
    /// If only the query_map and the current holder hold the context_query,
    /// the query has been completed and the query_context is released.
    if (context && context.use_count() == 2)
        cache->removeQueryContext(query_id);
}

}
