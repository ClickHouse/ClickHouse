#pragma once

#include <Common/FileCache_fwd.h>
#include <Common/FileCacheSettings.h>

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

        FileCacheData(FileCachePtr cache_, const FileCacheSettings & settings_) : cache(cache_), settings(settings_) {}
    };

    using Caches = std::list<FileCacheData>;
    using CacheByBasePath = std::unordered_map<std::string, Caches::iterator>;
    using CacheByName = std::unordered_map<std::string, Caches::iterator>;

    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_base_path, const FileCacheSettings & file_cache_settings, const std::string & name);

    FileCachePtr get(const std::string & cache_base_path);

    CacheByBasePath getAll();

    const FileCacheSettings & getSettings(const std::string & cache_base_path);

    FileCacheData getByName(const std::string & name);

    CacheByName getAllByName();

private:
    std::mutex mutex;
    Caches caches;

    CacheByBasePath caches_by_path;
    CacheByName caches_by_name;
};

}
