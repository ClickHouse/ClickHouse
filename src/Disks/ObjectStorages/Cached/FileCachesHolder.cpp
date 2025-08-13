#include <Disks/ObjectStorages/Cached/FileCachesHolder.h>
#include <Interpreters/Cache/FileCache.h>

namespace fs = std::filesystem;
namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

constexpr std::array<std::string, 1> data_cache_type = {".bin"};

std::string getSplitCacheTypeStr(SplitCacheType cache_type)
{
    switch (cache_type)
    {
        case (SplitCacheType::GeneralCache): return "GeneralCache";
        case (SplitCacheType::SystemCache): return "SystemCache";
        case (SplitCacheType::DataCache): return "DataCache";
    }
}

SplitCacheType defineCacheType(const std::string & file_path)
{
    auto file_extension = fs::path(file_path).extension();
    return std::find(data_cache_type.begin(), data_cache_type.end(), file_extension) != data_cache_type.end() ? SplitCacheType::DataCache : SplitCacheType::SystemCache;
}

FileCachesHolder::FileCachesHolder(std::initializer_list<std::tuple<SplitCacheType, FileCachePtr>> caches_)
{
    for (auto && [cache_type, cache] : caches_)
    {
        if (holder.contains(cache_type))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Multiple caches with the same type");
        }
        chassert(cache);
        holder[cache_type] = cache;
    }
}

void FileCachesHolder::setCache(SplitCacheType cache_type, const FileCachePtr & system_cache_)
{
    holder[cache_type] = system_cache_;
}

FileCachePtr FileCachesHolder::getCache(SplitCacheType cache_type) const
{
    if (!holder.contains(cache_type))
    {
        if (!holder.contains(SplitCacheType::GeneralCache))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no cache with type {}", static_cast<int>(cache_type));
        }
        return holder.at(SplitCacheType::GeneralCache);
    }
    return holder.at(cache_type);
}

bool FileCachesHolder::hasCache(SplitCacheType cache_type) const
{
    return holder.contains(cache_type);
}

void FileCachesHolder::checkCorrectness() const
{
    if (hasCache(SplitCacheType::GeneralCache) &&
        (hasCache(SplitCacheType::SystemCache) || hasCache(SplitCacheType::DataCache)))
    {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "SplitCacheType::GeneralCache should be the only cache in the holder");
    }
}

void FileCachesHolder::initializeAll()
{
    for (const auto & [_, cache] : holder)
    {
        if (!cache)
        {
            continue;
        }
        cache->initialize();
    }
}

}
