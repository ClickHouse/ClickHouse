#pragma once

#include <Common/FileCache_fwd.h>
#include <Common/FileCacheSettings.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <mutex>

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

    using CacheByBasePath = std::unordered_map<std::string, FileCacheData>;
    using CacheByName = std::unordered_map<std::string, FileCacheData>;

    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_base_path, const FileCacheSettings & file_cache_settings, const std::string & name);

    FileCachePtr get(const std::string & cache_base_path);

    CacheByBasePath getAll();

    const FileCacheSettings & getSettings(const std::string & cache_base_path);

    FileCacheData getByName(const std::string & name);

private:
    FileCacheData * getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &);
    void registerCacheByName(const std::string & name, const FileCacheData & cache_data);

    std::mutex mutex;
    CacheByBasePath caches_by_path;
    CacheByName caches_by_name;
};

}
