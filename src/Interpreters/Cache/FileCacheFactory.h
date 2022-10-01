#pragma once

#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileCacheSettings.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <mutex>
#include <list>

namespace DB
{

/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory final : private boost::noncopyable
{
public:
    struct FileCacheData
    {
        FileCachePtr cache;
        FileCacheSettings settings;

        FileCacheData() = default;
        FileCacheData(FileCachePtr cache_, const FileCacheSettings & settings_) : cache(cache_), settings(settings_) {}
    };

    using Caches = std::list<FileCacheData>;
    using CacheByName = std::unordered_map<std::string, Caches::iterator>;

    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_name, const FileCacheSettings & file_cache_settings);

    CacheByName getAll();

    const FileCacheData & getByName(const std::string & cache_name);

private:
    std::mutex mutex;
    Caches caches;
    CacheByName caches_by_name;
};

}
