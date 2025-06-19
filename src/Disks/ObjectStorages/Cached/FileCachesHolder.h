#pragma once
#include <array>
#include <initializer_list>
#include <unordered_map>
#include <map>
#include <Interpreters/Cache/FileCacheSettings.h>

namespace DB
{

enum CacheType
{
    GeneralCache = 0,
    SystemCache, // cache system data(metadata, index, marks, etc.)
    DataCache, // cache table data
    Size // used for count the amount of cache types(should be the last)
};

constexpr std::array<std::pair<std::string, CacheType>, CacheType::Size> file_suffix_to_cache_type = {std::pair("cidx", CacheType::SystemCache)};

CacheType defineCacheType(const std::string & file_path);

class FileCachesHolder
{
public:
    FileCachesHolder() = default;
    FileCachesHolder(std::initializer_list<std::tuple<CacheType, FileCachePtr, FileCacheSettings>> caches_);
    void setCache(CacheType cache_type, const FileCachePtr & system_cache_, FileCacheSettings&& system_cache_settings_);
    FileCachePtr getCache(CacheType cache_type) const;
    const FileCacheSettings & getCacheSetting(CacheType cache_type) const;
    void initializeAll();
    void checkCorrectness() const;
private:
    std::array<std::pair<FileCachePtr, FileCacheSettings>, CacheType::Size> holder;
};

}
