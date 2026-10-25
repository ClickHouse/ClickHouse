#pragma once

#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/FileCacheSettings.h>

#include <boost/noncopyable.hpp>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <vector>

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
    using Caches = std::unordered_set<FileCacheDataPtr>;

    static FileCacheFactory & instance();

    /// `is_custom_disk` is set for the cache of a custom cache disk (see `disk(type = cache, ...)`): the name is
    /// remembered if it is added by this call, so that `removeNameOfReleasedDisk` removes only the names the
    /// custom disks added.
    FileCachePtr getOrCreate(
        const std::string & cache_name,
        const FileCacheSettings & file_cache_settings,
        const std::string & config_path,
        bool is_custom_disk = false);

    FileCachePtr get(const std::string & cache_name);

    FileCachePtr create(
        const std::string & cache_name,
        const FileCacheSettings & file_cache_settings,
        const std::string & config_path);

    CacheByName getAll();
    Caches getUniqueInstances();

    FileCacheDataPtr getByName(const std::string & cache_name);

    void loadDefaultCaches(const Poco::Util::AbstractConfiguration & config, ContextPtr context);

    void updateSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);

    void remove(FileCachePtr cache);

    /// Removes the name `cache_name` of the cache of a custom cache disk (see `disk(type = cache, ...)`) that has been shut down,
    /// so that the name can be used again, possibly with another path. Only a name added by a custom disk is removed, so a name
    /// from the configuration stays even if a custom disk has the same name. A cache that has other names stays under them.
    /// A cache with no other name is removed only if it was not defined in the configuration and nothing else uses it anymore,
    /// including the snapshots returned by `getAll`, so that a new cache is never created for a path still used by the old one.
    /// If something still uses it, the name is removed by a later call of a method of the factory, once nothing does.
    void removeNameOfReleasedDisk(const std::string & cache_name);

    void clear();

private:
    std::mutex mutex;
    CacheByName caches_by_name;
    /// The names in `caches_by_name` added by custom cache disks.
    std::unordered_set<std::string> names_of_custom_disks;
    /// The names in `names_of_custom_disks` whose disks were released, but whose caches were still used by something
    /// else at that moment, see `removeNameOfReleasedDisk`.
    std::unordered_set<std::string> names_of_released_disks;

    /// Removes the names in `names_of_released_disks` whose caches are not used anymore. The removed caches are moved
    /// to `removed`, to be destroyed by the caller after unlocking `mutex`.
    void removeUnusedNamesOfReleasedDisks(std::vector<FileCacheDataPtr> & removed);
};

/// Get default path prefix for non-disk caches.
/// For disk we have a similar method in registerDiskCache.
std::string getPathPrefixForRelativeCachePath(ContextPtr context);

}
