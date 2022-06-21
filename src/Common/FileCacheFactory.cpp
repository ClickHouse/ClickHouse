#include "FileCacheFactory.h"
#include "LRUFileCache.h"

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
    return caches_by_path;
}

const FileCacheSettings & FileCacheFactory::getSettings(const std::string & cache_base_path)
{
    std::lock_guard lock(mutex);

    auto * cache_data = getImpl(cache_base_path, lock);
    if (cache_data)
        return cache_data->settings;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "No cache found by path: {}", cache_base_path);
}

FileCacheFactory::FileCacheData * FileCacheFactory::getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &)
{
    auto it = caches_by_path.find(cache_base_path);
    if (it == caches_by_path.end())
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
    const std::string & cache_base_path, const FileCacheSettings & file_cache_settings, const std::string & name)
{
    std::lock_guard lock(mutex);

    auto * cache_data = getImpl(cache_base_path, lock);
    if (cache_data)
    {
        registerCacheByName(name, *cache_data);
        return cache_data->cache;
    }

    auto cache = std::make_shared<LRUFileCache>(cache_base_path, file_cache_settings);
    FileCacheData result{cache, file_cache_settings};

    registerCacheByName(name, result);
    caches_by_path.emplace(cache_base_path, result);

    return cache;
}

FileCacheFactory::FileCacheData FileCacheFactory::getByName(const std::string & name)
{
    auto it = caches_by_name.find(name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No cache found by name: {}", name);
    return it->second;
}

void FileCacheFactory::registerCacheByName(const std::string & name, const FileCacheData & cache_data)
{
    caches_by_name.emplace(std::make_pair(name, cache_data));
}

}
