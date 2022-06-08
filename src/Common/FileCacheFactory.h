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
    struct CacheData
    {
        FileCachePtr cache;
        FileCacheSettings settings;

        CacheData(FileCachePtr cache_, const FileCacheSettings & settings_) : cache(cache_), settings(settings_) {}
    };

    using CacheByBasePath = std::unordered_map<std::string, CacheData>;

public:
    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_base_path, const FileCacheSettings & file_cache_settings);

    FileCachePtr get(const std::string & cache_base_path);

    CacheByBasePath getAll();

    const FileCacheSettings & getSettings(const std::string & cache_base_path);

private:
    CacheData * getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &);

    std::mutex mutex;
    CacheByBasePath caches;
};

}
