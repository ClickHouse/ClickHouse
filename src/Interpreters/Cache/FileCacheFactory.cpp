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

FileCacheFactory::CacheByName FileCacheFactory::getAll()
{
    std::lock_guard lock(mutex);
    return caches_by_name;
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_name, const FileCacheSettings & file_cache_settings)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
    {
        auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);
        it = caches_by_name.emplace(
            cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings)).first;
    }

    return it->second->cache;
}

FileCacheFactory::FileCacheData FileCacheFactory::getByName(const std::string & cache_name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no cache by name: {}", cache_name);

    return *it->second;
}

}
