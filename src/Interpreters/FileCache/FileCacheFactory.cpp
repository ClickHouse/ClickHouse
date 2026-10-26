#include <Interpreters/FileCache/FileCacheFactory.h>
#include <Interpreters/FileCache/FileCache.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace FileCacheSetting
{
    extern const FileCacheSettingsString path;
}

FileCacheFactory::FileCacheData::FileCacheData(
    FileCachePtr cache_,
    const FileCacheSettings & settings_,
    const std::string & config_path_)
    : cache(cache_)
    , config_path(config_path_)
    , settings(settings_)
{
}

FileCacheSettings FileCacheFactory::FileCacheData::getSettings() const
{
    std::lock_guard lock(settings_mutex);
    return settings;
}

void FileCacheFactory::FileCacheData::setSettings(FileCacheSettings && new_settings)
{
    std::lock_guard lock(settings_mutex);
    settings = std::move(new_settings);
}

FileCacheFactory & FileCacheFactory::instance()
{
    static FileCacheFactory ret;
    return ret;
}

FileCacheFactory::CacheByName FileCacheFactory::getAll()
{
    RemovedCaches removed(*this);
    std::lock_guard lock(mutex);
    removeUnusedNamesOfReleasedDisks(removed);
    return caches_by_name;
}

FileCacheFactory::Caches FileCacheFactory::getUniqueInstances()
{
    RemovedCaches removed(*this);
    std::lock_guard lock(mutex);
    removeUnusedNamesOfReleasedDisks(removed);
    Caches caches;
    for (const auto & [_, cache_data] : caches_by_name)
        caches.insert(cache_data);
    return caches;
}

FileCachePtr FileCacheFactory::get(const std::string & cache_name)
{
    RemovedCaches removed(*this);
    std::lock_guard lock(mutex);
    removeUnusedNamesOfReleasedDisks(removed);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no cache by name `{}`", cache_name);
    return it->second->cache;
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_name,
    const FileCacheSettings & file_cache_settings,
    const std::string & config_path,
    bool is_custom_disk)
{
    RemovedCaches removed(*this);
    std::unique_lock lock(mutex);
    waitForCacheDestroyed(lock, file_cache_settings[FileCacheSetting::path].value, removed);

    auto it = std::find_if(caches_by_name.begin(), caches_by_name.end(), [&](const auto & cache_by_name)
    {
        return cache_by_name.second->getSettings()[FileCacheSetting::path].value == file_cache_settings[FileCacheSetting::path].value;
    });

    if (it == caches_by_name.end())
    {
        auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);

        bool inserted = false;
        std::tie(it, inserted) = caches_by_name.emplace(
            cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings, config_path));

        if (!inserted)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cache with name {} exists, but it has a different path", cache_name);
        }

        if (is_custom_disk)
            names_of_custom_disks.insert(cache_name);
    }
    else if (it->second->getSettings() != file_cache_settings)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Found more than one cache configuration with the same path, "
                        "but with different cache settings ({} and {})",
                        it->first, cache_name);
    }
    else if (it->first != cache_name)
    {
        if (caches_by_name.emplace(cache_name, it->second).second && is_custom_disk)
            names_of_custom_disks.insert(cache_name);
    }

    /// The name of a released disk is used again, so it is not removed anymore.
    if (is_custom_disk)
        names_of_released_disks.erase(cache_name);

    return it->second->cache;
}

FileCachePtr FileCacheFactory::create(
    const std::string & cache_name,
    const FileCacheSettings & file_cache_settings,
    const std::string & config_path)
{
    RemovedCaches removed(*this);
    std::unique_lock lock(mutex);
    waitForCacheDestroyed(lock, file_cache_settings[FileCacheSetting::path].value, removed);

    auto it = caches_by_name.find(cache_name);

    if (it != caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache with name {} already exists", cache_name);

    it = std::find_if(caches_by_name.begin(), caches_by_name.end(), [&](const auto & cache_by_name)
    {
        return cache_by_name.second->getSettings()[FileCacheSetting::path].value == file_cache_settings[FileCacheSetting::path].value;
    });

    if (it == caches_by_name.end())
    {
        auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);
        it = caches_by_name.emplace(
            cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings, config_path)).first;
    }
    else if (it->second->getSettings() != file_cache_settings)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Found more than one cache configuration with the same path, "
                        "but with different cache settings ({} and {})",
                        it->first, cache_name);
    }
    else
    {
        [[maybe_unused]] bool inserted = caches_by_name.emplace(cache_name, it->second).second;
        chassert(inserted);
    }

    return it->second->cache;
}

FileCacheFactory::FileCacheDataPtr FileCacheFactory::getByName(const std::string & cache_name)
{
    RemovedCaches removed(*this);
    std::lock_guard lock(mutex);
    removeUnusedNamesOfReleasedDisks(removed);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no cache by name: {}", cache_name);

    return it->second;
}

void FileCacheFactory::updateSettingsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    CacheByName caches_by_name_copy;
    {
        std::lock_guard lock(mutex);
        caches_by_name_copy = caches_by_name;
    }

    std::unordered_set<std::string> checked_paths;
    for (const auto & [_, cache_info] : caches_by_name_copy)
    {
        if (cache_info->config_path.empty() || checked_paths.contains(cache_info->config_path))
            continue;

        checked_paths.emplace(cache_info->config_path);

        FileCacheSettings old_settings = cache_info->getSettings();

        FileCacheSettings new_settings;
        new_settings.loadFromConfig(config, cache_info->config_path, /* cache_path_prefix_if_relative */"/no-op/");

        /// `path` setting can never be changed (in applySettingsIfPossilbe below)
        /// but it will differ here even if in fact equal,
        /// because of relative path usage in config,
        /// while in old_settings it would already be normalized into absolute path.
        /// We cannot do the same here for new_settings
        /// (as we do not know if they are disk settings old non-disk settings,
        /// while they have different default path prefix),
        /// so consider them always equal as anyway non-changeable.
        new_settings[FileCacheSetting::path] = old_settings[FileCacheSetting::path];

        if (old_settings == new_settings)
        {
            continue;
        }

        /// FIXME: registerDiskCache modifies `path` setting of FileCacheSettings if path is relative.
        /// This can lead to calling applySettingsIfPossible even though nothing changed, which is avoidable.

        // LOG_TRACE(log, "Will apply settings changes for cache {}. "
        //           "Settings changes: {} (new settings: {}, old_settings: {})",
        //           cache_name, fmt::join(new_settings.getSettingsDiff(old_settings), ", "),
        //           new_settings.toString(), old_settings.toString());

        try
        {
            cache_info->cache->applySettingsIfPossible(new_settings, old_settings);
        }
        catch (...)
        {
            /// Settings changes could be partially applied in case of exception,
            /// make sure cache_info->settings show correct state of applied settings.
            cache_info->setSettings(std::move(old_settings));
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }

        cache_info->setSettings(std::move(old_settings));
    }
}

void FileCacheFactory::remove(FileCachePtr cache)
{
    std::lock_guard lock(mutex);
    for (auto it = caches_by_name.begin(); it != caches_by_name.end();)
    {
        if (it->second->cache == cache)
        {
            names_of_custom_disks.erase(it->first);
            names_of_released_disks.erase(it->first);
            it = caches_by_name.erase(it);
        }
        else
            ++it;
    }
}

void FileCacheFactory::removeNameOfReleasedDisk(const std::string & cache_name)
{
    /// Destroying the cache stops its background operations, which is not done under `mutex`.
    RemovedCaches removed(*this);

    std::lock_guard lock(mutex);
    if (!names_of_custom_disks.contains(cache_name))
        return;

    chassert(caches_by_name.contains(cache_name));
    names_of_released_disks.insert(cache_name);
    removeUnusedNamesOfReleasedDisks(removed);
}

void FileCacheFactory::RemovedCaches::destroy()
{
    caches.clear();
    if (paths.empty())
        return;

    std::lock_guard lock(factory.mutex);
    for (const auto & path : paths)
        factory.paths_of_caches_being_destroyed.erase(factory.paths_of_caches_being_destroyed.find(path));
    paths.clear();
    factory.cache_destroyed_cv.notify_all();
}

void FileCacheFactory::waitForCacheDestroyed(std::unique_lock<std::mutex> & lock, const std::string & path, RemovedCaches & removed)
{
    while (true)
    {
        removeUnusedNamesOfReleasedDisks(removed);
        if (!paths_of_caches_being_destroyed.contains(path))
            return;

        if (removed.caches.empty())
        {
            /// Another thread is destroying the cache.
            cache_destroyed_cv.wait(lock);
        }
        else
        {
            lock.unlock();
            removed.destroy();
            lock.lock();
        }
    }
}

void FileCacheFactory::removeUnusedNamesOfReleasedDisks(RemovedCaches & removed)
{
    for (auto name_it = names_of_released_disks.begin(); name_it != names_of_released_disks.end();)
    {
        const auto & cache_name = *name_it;
        auto it = caches_by_name.find(cache_name);
        if (it == caches_by_name.end())
        {
            name_it = names_of_released_disks.erase(name_it);
            continue;
        }

        const bool has_other_names = std::any_of(caches_by_name.begin(), caches_by_name.end(), [&](const auto & cache_by_name)
        {
            return cache_by_name.first != cache_name && cache_by_name.second == it->second;
        });

        if (!has_other_names)
        {
            /// A cache defined in the configuration stays.
            if (!it->second->config_path.empty())
            {
                name_it = names_of_released_disks.erase(name_it);
                continue;
            }

            /// Something still uses the cache, so its name is removed later, when it does not anymore. The snapshots returned
            /// by `getAll` share the `FileCacheData`, not the `FileCachePtr`, so both are checked.
            if (it->second.use_count() != 1 || it->second->cache.use_count() != 1)
            {
                ++name_it;
                continue;
            }
        }

        if (!has_other_names)
        {
            auto path = it->second->getSettings()[FileCacheSetting::path].value;
            paths_of_caches_being_destroyed.insert(path);
            removed.paths.push_back(std::move(path));
        }
        removed.caches.push_back(std::move(it->second));
        caches_by_name.erase(it);
        names_of_custom_disks.erase(cache_name);
        name_it = names_of_released_disks.erase(name_it);
    }
}

void FileCacheFactory::clear()
{
    std::lock_guard lock(mutex);
    caches_by_name.clear();
    names_of_custom_disks.clear();
    names_of_released_disks.clear();
}

void FileCacheFactory::loadDefaultCaches(const Poco::Util::AbstractConfiguration & config, ContextPtr context)
{
    Poco::Util::AbstractConfiguration::Keys cache_names;
    config.keys(FILECACHE_DEFAULT_CONFIG_PATH, cache_names);

    auto * log = &Poco::Logger::get("FileCacheFactory");
    LOG_DEBUG(log, "Will load {} caches from default cache config", cache_names.size());

    for (const auto & name : cache_names)
    {
        const auto config_path = fmt::format("{}.{}", FILECACHE_DEFAULT_CONFIG_PATH, name);

        FileCacheSettings settings;
        settings.loadFromConfig(
            config,
            config_path,
            getPathPrefixForRelativeCachePath(context),
            /* default_cache_path */"");

        auto cache = getOrCreate(name, settings, config_path);
        cache->initialize();

        LOG_DEBUG(log, "Loaded cache `{}` from default cache config", name);
    }
}

std::string getPathPrefixForRelativeCachePath(ContextPtr context)
{
    auto config_fs_caches_dir = context->getFilesystemCachesPath();
    if (!config_fs_caches_dir.empty())
        return config_fs_caches_dir;

    return fs::path(context->getPath()) / "caches";
}

}
