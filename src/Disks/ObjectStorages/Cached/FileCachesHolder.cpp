#include <Disks/ObjectStorages/Cached/FileCachesHolder.h>
#include <Interpreters/Cache/FileCache.h>

namespace fs = std::filesystem;
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


SplitCacheType defineCacheType(const std::string & file_path)
{
    auto file_extension = fs::path(file_path).extension();
    return std::find(data_cache_type.begin(), data_cache_type.end(), file_extension) != data_cache_type.end() ? SplitCacheType::DataCache : SplitCacheType::SystemCache;
}

FileCachesHolder::FileCachesHolder(std::initializer_list<std::tuple<SplitCacheType, FileCachePtr, FileCacheSettings>> caches_)
{
    for (auto&& [cache_type, cache, cache_settings] : caches_)
    {
        if (holder.contains(cache_type))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Multiple caches with the same type");
        }
        holder[cache_type] = {cache, cache_settings};
    }
}

void FileCachesHolder::setCache(SplitCacheType cache_type, const FileCachePtr & system_cache_, FileCacheSettings&& system_cache_settings_)
{
    holder[cache_type].first = system_cache_;
    holder[cache_type].second = std::move(system_cache_settings_);
}

FileCachePtr FileCachesHolder::getCache(SplitCacheType cache_type) const
{
    if (!holder.contains(cache_type))
    {
        if (!holder.contains(SplitCacheType::GeneralCache))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get cache with non-existing cache_type");
        }
        return holder.at(SplitCacheType::GeneralCache).first;
    }
    return holder.at(cache_type).first;
}

const FileCacheSettings & FileCachesHolder::getCacheSetting(SplitCacheType cache_type) const
{
    if (!holder.contains(cache_type))
    {
        if (!holder.contains(SplitCacheType::GeneralCache))
        {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to get cache settings with non-existing cache_type");
        }
        return holder.at(SplitCacheType::GeneralCache).second;
    }
    return holder.at(cache_type).second;
}

void FileCachesHolder::checkCorrectness() const
{
    if (getCache(SplitCacheType::GeneralCache) &&
        (getCache(SplitCacheType::SystemCache) || getCache(SplitCacheType::DataCache)))
    {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitCacheType::GeneralCache should be the only cache in holder");
    }
}

void FileCachesHolder::initializeAll()
{
    for (auto&& [_, cache_info] : holder)
    {
        if (!cache_info.first)
        {
            continue;
        }
        cache_info.first->initialize();
    }
}

}
