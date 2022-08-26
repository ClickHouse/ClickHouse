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
    return caches_by_path;
}

const FileCacheSettings & FileCacheFactory::getSettings(const std::string & cache_base_path)
{
    std::lock_guard lock(mutex);
    auto it = caches_by_path.find(cache_base_path);
    if (it == caches_by_path.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No cache found by path: {}", cache_base_path);
    return it->second->settings;

}

bool FileCacheFactory::tryGetByPath(FileCacheData & result, const std::string & cache_path)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_path.find(cache_path);
    if (it == caches_by_path.end())
        return false;

    result = *it->second;
    return true;

}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_base_path, const FileCacheSettings & file_cache_settings, const std::string & name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_path.find(cache_base_path);
    if (it != caches_by_path.end())
    {
        caches_by_name.emplace(name, it->second);
        return it->second->cache;
    }

    auto cache = std::make_shared<FileCache>(cache_base_path, file_cache_settings);
    FileCacheData result{cache, file_cache_settings};

    auto cache_it = caches.insert(caches.end(), std::move(result));
    caches_by_name.emplace(name, cache_it);
    caches_by_path.emplace(cache_base_path, cache_it);

    return cache;
}

bool FileCacheFactory::tryGetByName(FileCacheData & result, const std::string & cache_name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        return false;

    result = *it->second;
    return true;
}

FileCacheFactory::CacheByName FileCacheFactory::getAllByName()
{
    std::lock_guard lock(mutex);
    return caches_by_name;
}

}
