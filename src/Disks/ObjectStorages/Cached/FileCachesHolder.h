#pragma once
#include <array>
#include <initializer_list>
#include <unordered_map>
#include <Interpreters/Cache/FileCacheSettings.h>

namespace DB
{

enum SplitCacheType
{
    GeneralCache = 0,
    SystemCache, // cache system data(metadata, index, marks, etc.)
    DataCache // cache for table data
};

constexpr std::array<std::string, 1> data_type_extensions = {".bin"};
SplitCacheType defineCacheType(const std::string & file_path);

class FileCachesHolder
{
public:
    FileCachesHolder() = default;
    FileCachesHolder(std::initializer_list<std::tuple<SplitCacheType, FileCachePtr, FileCacheSettings>> caches_);
    void setCache(SplitCacheType cache_type, const FileCachePtr & system_cache_, FileCacheSettings&& system_cache_settings_);
    FileCachePtr getCache(SplitCacheType cache_type) const;
    const FileCacheSettings & getCacheSetting(SplitCacheType cache_type) const;
    void initializeAll();
    void checkCorrectness() const;
private:
    std::unordered_map<SplitCacheType, std::pair<FileCachePtr, FileCacheSettings>> holder;
};

}
