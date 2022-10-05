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

FileCacheFactory::Caches FileCacheFactory::getAll()
{
    std::lock_guard lock(mutex);
    return caches;
}

FileCacheFactory::CacheByName FileCacheFactory::getAllByName()
{
    std::lock_guard lock(mutex);
    return caches_by_name;
}

FileCachePtr FileCacheFactory::getOrCreate(const std::string & cache_name, const FileCacheSettings & file_cache_settings)
{
    std::lock_guard lock(mutex);

    auto [it, inserted] = caches_by_path.emplace(file_cache_settings.base_path, Caches::iterator{});
    if (inserted)
    {
        it->second = caches.insert(
            caches.end(),
            FileCacheData{std::make_shared<FileCache>(file_cache_settings), file_cache_settings});
    }

    /// It can be a different cache, which shares the same cache path => the same cache object,
    /// so need to save only by cache name if !inserted.
    /// Here we could either into caches_by_name emplace or not -- both are possible and ok.
    caches_by_name.emplace(cache_name, it->second);

    return it->second->cache;
}

FileCachePtr FileCacheFactory::create(const std::string & cache_name, const FileCacheSettings & file_cache_settings)
{
    std::lock_guard lock(mutex);

    auto [it, inserted] = caches_by_name.emplace(cache_name, Caches::iterator{});
    if (!inserted)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache already exists by name: {}", cache_name);

    it->second = caches.insert(
        caches.end(),
        FileCacheData{std::make_shared<FileCache>(file_cache_settings), file_cache_settings});

    inserted = caches_by_path.emplace(file_cache_settings.base_path, it->second).second;
    assert(inserted);

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

std::optional<FileCacheFactory::FileCacheData> FileCacheFactory::tryGetByName(const std::string & cache_name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        return std::nullopt;

    return *it->second;
}

}
