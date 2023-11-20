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
    using FileCacheDataPtr = std::shared_ptr<FileCacheData>;
    using CacheByName = std::unordered_map<std::string, FileCacheDataPtr>;

    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(const std::string & cache_name, const FileCacheSettings & file_cache_settings);

    CacheByName getAll();

    FileCacheData getByName(const std::string & cache_name);

private:
    std::mutex mutex;
    CacheByName caches_by_name;
};

}
