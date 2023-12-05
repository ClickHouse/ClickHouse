#include "FileCacheFactory.h"
#include "FileCache.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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

FileCacheFactory::FileCacheData FileCacheFactory::getByName(const std::string & cache_name)
{
    std::lock_guard lock(mutex);

    auto it = caches_by_name.find(cache_name);
    if (it == caches_by_name.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no cache by name: {}", cache_name);

    return *it->second;
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

        FileCacheSettings settings;
        settings.loadFromConfig(config, cache_info->config_path);

        if (settings == cache_info->settings)
            continue;

        cache_info->settings = cache_info->cache->applySettingsIfPossible(settings);
    }
}

}
