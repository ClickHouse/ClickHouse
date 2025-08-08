#pragma once

#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/FileCacheSettings.h>

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
    class FileCacheData
    {
        friend class FileCacheFactory;
    public:
        FileCacheData(FileCachePtr cache_, const FileCacheSettings & settings_, const std::string & config_path_);

        FileCacheSettings getSettings() const;

        void setSettings(FileCacheSettings && new_settings);

        const FileCachePtr cache;
        const std::string config_path;

    private:
        FileCacheSettings settings;
        mutable std::mutex settings_mutex;
    };

    using FileCacheDataPtr = std::shared_ptr<FileCacheData>;
    using CacheByName = std::unordered_map<std::string, FileCacheDataPtr>;

    static FileCacheFactory & instance();

    FileCachePtr getOrCreate(
        const std::string & cache_name,
        const FileCacheSettings & file_cache_settings,
        const std::string & config_path);

    FileCachePtr get(const std::string & cache_name);

    FileCachePtr create(
        const std::string & cache_name,
        const FileCacheSettings & file_cache_settings,
        const std::string & config_path);

    CacheByName getAll();

    FileCacheDataPtr getByName(const std::string & cache_name);

    void loadDefaultCaches(const Poco::Util::AbstractConfiguration & config, ContextPtr context);

    void updateSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);

    void remove(FileCachePtr cache);

    void clear();

private:
    std::mutex mutex;
    CacheByName caches_by_name;
};

/// Get default path prefix for non-disk caches.
/// For disk we have a similar method in registerDiskCache.
std::string getPathPrefixForRelativeCachePath(ContextPtr context);

}
