#include "FileCacheFactory.h"
#include "FileCache.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

FileCacheFactory & FileCacheFactory::instance()
{
    static FileCacheFactory ret;
    return ret;
}

FileCacheFactory::CacheByBasePath FileCacheFactory::getAll()
{
    std::lock_guard lock(mutex);
    return caches;
}

const FileCacheSettings & FileCacheFactory::getSettings(const std::string & cache_base_path)
{
    std::lock_guard lock(mutex);

    auto * cache_data = getImpl(cache_base_path, lock);
    if (cache_data)
        return cache_data->settings;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "No cache found by path: {}", cache_base_path);
}

FileCacheFactory::CacheData * FileCacheFactory::getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &)
{
    auto it = caches.find(cache_base_path);
    if (it == caches.end())
        return nullptr;
    return &it->second;
}

FileCachePtr FileCacheFactory::get(const std::string & cache_base_path)
{
    std::lock_guard lock(mutex);

    auto * cache_data = getImpl(cache_base_path, lock);
    if (cache_data)
        return cache_data->cache;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "No cache found by path: {}", cache_base_path);
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_base_path, const FileCacheSettings & file_cache_settings)
{
    std::lock_guard lock(mutex);

    auto * cache_data = getImpl(cache_base_path, lock);
    if (cache_data)
        return cache_data->cache;

    auto cache = std::make_shared<LRUFileCache>(cache_base_path, file_cache_settings);
    caches.emplace(cache_base_path, CacheData(cache, file_cache_settings));
    return cache;
}

}
