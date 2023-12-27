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

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
    {
        auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);
        it = caches_by_name.emplace(
            cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings, config_path)).first;
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

    auto cache = std::make_shared<FileCache>(cache_name, file_cache_settings);
    it = caches_by_name.emplace(
        cache_name, std::make_unique<FileCacheData>(cache, file_cache_settings, config_path)).first;

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

    for (const auto & [_, cache_info] : caches_by_name_copy)
    {
        if (cache_info->config_path.empty())
            continue;

        FileCacheSettings new_settings;
        new_settings.loadFromConfig(config, cache_info->config_path);

        FileCacheSettings old_settings = cache_info->getSettings();
        if (old_settings == new_settings)
            continue;

        try
        {
            cache_info->cache->applySettingsIfPossible(new_settings, old_settings);
        }
        catch (...)
        {
            /// Settings changes could be partially applied in case of exception,
            /// make sure cache_info->settings show correct state of applied settings.
            cache_info->setSettings(old_settings);
            throw;
        }

        cache_info->setSettings(old_settings);
    }
}

}
