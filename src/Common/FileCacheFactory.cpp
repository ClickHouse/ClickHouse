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

FileCachePtr FileCacheFactory::getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &)
{
    auto it = caches.find(cache_base_path);
    if (it == caches.end())
        return nullptr;
    return it->second;
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_base_path, size_t max_size, size_t max_elements_size, size_t max_file_segment_size)
{
    std::lock_guard lock(mutex);
    auto cache = getImpl(cache_base_path, lock);
    if (cache)
    {
        if (cache->capacity() != max_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache with path `{}` already exists, but has different max size", cache_base_path);
        return cache;
    }

    cache = std::make_shared<LRUFileCache>(cache_base_path, max_size, max_elements_size, max_file_segment_size);
    caches.emplace(cache_base_path, cache);
    return cache;
}

}
