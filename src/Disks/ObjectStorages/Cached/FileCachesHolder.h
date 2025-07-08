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
    DataCache, // cache table data
};

std::string getSplitCacheTypeStr(SplitCacheType cache_type);

SplitCacheType defineCacheType(const std::string & file_path);

/// A class, which can store several FileCache objects
/// with a SplitCacheType defining access to them.
class FileCachesHolder
{
public:
    FileCachesHolder() = default;
    FileCachesHolder(std::initializer_list<std::tuple<SplitCacheType, FileCachePtr>> caches_);
    FileCachesHolder(FileCachesHolder && file_caches_holder_) = default;
    FileCachesHolder & operator=(FileCachesHolder && file_caches_holder_) = default;

    void setCache(SplitCacheType cache_type, const FileCachePtr & system_cache_);
    FileCachePtr getCache(SplitCacheType cache_type) const;
    bool hasCache(SplitCacheType cache_type) const;
    void initializeAll();
    void checkCorrectness() const;
private:
    std::unordered_map<SplitCacheType, FileCachePtr> holder;
};

}
