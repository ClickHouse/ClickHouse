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

    auto [it, inserted] = caches_by_name.emplace(cache_name, Caches::iterator{});
    if (inserted)
    {
        it->second = caches.insert(
            caches.end(),
            FileCacheData{std::make_shared<FileCache>(file_cache_settings), file_cache_settings});
    }

    return it->second->cache;
}

const FileCacheFactory::FileCacheData & FileCacheFactory::getByName(const std::string & cache_name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no cache by name: {}", cache_name);

    return *it->second;
}

}
