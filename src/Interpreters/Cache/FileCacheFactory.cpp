#include "FileCacheFactory.h"
#include "FileCache.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

void FileCacheFactory::FileCacheData::setSettings(const FileCacheSettings & new_settings)
{
    std::lock_guard lock(settings_mutex);
    settings = new_settings;
}

FileCacheFactory & FileCacheFactory::instance()
{
    static FileCacheFactory ret;
    return ret;
}

FileCacheFactory::CacheByName FileCacheFactory::getAll()
{
    std::lock_guard lock(mutex);
    return caches_by_name;
}

FileCachePtr FileCacheFactory::getOrCreate(
    const std::string & cache_name,
    const FileCacheSettings & file_cache_settings,
    const std::string & config_path)
{
    std::lock_guard lock(mutex);

    auto it = std::find_if(caches_by_name.begin(), caches_by_name.end(), [&](const auto & cache_by_name)
    {
        return cache_by_name.second->getSettings().base_path == file_cache_settings.base_path;
    });

    if (it == caches_by_name.end())
    {
        auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);

        bool inserted;
        std::tie(it, inserted) = caches_by_name.emplace(
            cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings, config_path));

        if (!inserted)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Cache with name {} exists, but it has a different path", cache_name);
        }
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
        caches_by_name.emplace(cache_name, it->second);
    }

    return it->second->cache;
}

FileCachePtr FileCacheFactory::create(
    const std::string & cache_name,
    const FileCacheSettings & file_cache_settings,
    const std::string & config_path)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);

    if (it != caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache with name {} already exists", cache_name);

    it = std::find_if(caches_by_name.begin(), caches_by_name.end(), [&](const auto & cache_by_name)
    {
        return cache_by_name.second->getSettings().base_path == file_cache_settings.base_path;
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
    std::lock_guard lock(mutex);

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

        FileCacheSettings new_settings;
        new_settings.loadFromConfig(config, cache_info->config_path);

        FileCacheSettings old_settings = cache_info->getSettings();
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
            cache_info->setSettings(old_settings);
            tryLogCurrentException(__PRETTY_FUNCTION__);
            throw;
        }

        cache_info->setSettings(old_settings);
    }
}

void FileCacheFactory::remove(FileCachePtr cache)
{
    std::lock_guard lock(mutex);
    for (auto it = caches_by_name.begin(); it != caches_by_name.end();)
    {
        if (it->second->cache == cache)
            it = caches_by_name.erase(it);
        else
            ++it;
    }
}

void FileCacheFactory::clear()
{
    std::lock_guard lock(mutex);
    caches_by_name.clear();
}

}
