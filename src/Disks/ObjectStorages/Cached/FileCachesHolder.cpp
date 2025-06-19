#include <Disks/ObjectStorages/Cached/FileCachesHolder.h>
#include <Interpreters/Cache/FileCache.h>
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

CacheType defineCacheType(const std::string & file_path)
{
    size_t dot_pos = file_path.find_last_of('.');
    std::string_view file_extension(file_path.begin(), file_path.begin() + dot_pos);
    chassert(dot_pos != file_path.size());
    auto it = std::find_if(file_suffix_to_cache_type.begin(),
                        file_suffix_to_cache_type.end(),
                        [&] (const std::pair<std::string, CacheType> & p)
                        {
                            return file_extension == p.first;
                        });
    if (it == file_suffix_to_cache_type.end())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cache file has no extension");
    }
    return it->second;
}

FileCachesHolder::FileCachesHolder(std::initializer_list<std::tuple<CacheType, FileCachePtr, FileCacheSettings>> caches_)
{
    for (auto&& [cache_type, cache, cache_settings] : caches_)
    {
        if (holder[cache_type].first)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Multiple caches with the same type");
        }
        if (cache_type == CacheType::Size)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CacheType::Size is not a cache type(read the comment for enum)");
        }
        holder[cache_type] = {std::move(cache), std::move(cache_settings)};
    }
}

void FileCachesHolder::setCache(CacheType cache_type, const FileCachePtr & system_cache_, FileCacheSettings&& system_cache_settings_)
{
    holder[cache_type].first = system_cache_;
    holder[cache_type].second = std::move(system_cache_settings_);
}

FileCachePtr FileCachesHolder::getCache(CacheType cache_type) const
{
    return holder[cache_type].first;
}

const FileCacheSettings & FileCachesHolder::getCacheSetting(CacheType cache_type) const
{
    return holder[cache_type].second;
}

void FileCachesHolder::checkCorrectness() const
{
    if (getCache(CacheType::GeneralCache) &&
        (getCache(CacheType::SystemCache) || getCache(CacheType::DataCache)))
    {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "CacheType::GeneralCache should be the only cache in holder");
    }
}

void FileCachesHolder::initializeAll()
{
    for (auto&& [cache, cache_settings] : holder)
    {
        if (!cache)
        {
            continue;
        }
        cache->initialize();
    }
}

}
