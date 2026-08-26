#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>


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

std::pair<FileCachePtr, FileCacheSettings> getCache(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const ContextPtr & context,
    const std::string & cache_name,
    bool is_attach,
    bool is_custom_disk)
{
    FileCacheSettings file_cache_settings;
    auto predefined_configuration = config.has("cache_name")
        ? NamedCollectionFactory::instance().tryGet(config.getString("cache_name"))
        : nullptr;

    std::string cache_path_prefix_if_relative;
    std::string cache_path_prefix_if_absolute;
    auto config_fs_caches_dir = context->getFilesystemCachesPath();
    if (is_custom_disk)
    {
        static constexpr auto custom_cached_disks_base_dir_in_config = "custom_cached_disks_base_directory";
        auto custom_cached_disk_path_prefix = context->getConfigRef().getString(
            custom_cached_disks_base_dir_in_config,
            config_fs_caches_dir);

        if (custom_cached_disk_path_prefix.empty())
        {
            if (!is_attach)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot create cached custom disk without either "
                    "`filesystem_caches_path` (common for all filesystem caches) or"
                    "`custom_cached_disks_base_directory` (common only for custom cached disks) "
                    "in server configuration file");
            }
            /// Compatibility prefix.
            cache_path_prefix_if_relative = fs::path(context->getPath()) / "caches";
        }
        else
        {
            cache_path_prefix_if_relative = cache_path_prefix_if_absolute = fs::path(custom_cached_disk_path_prefix);
        }
    }
    else if (!config_fs_caches_dir.empty())
    {
        cache_path_prefix_if_relative = cache_path_prefix_if_absolute = config_fs_caches_dir;
    }
    else
    {
        cache_path_prefix_if_relative =  fs::path(context->getPath()) / "caches";
    }

    if (predefined_configuration)
        file_cache_settings.loadFromCollection(*predefined_configuration, cache_path_prefix_if_relative);
    else
        file_cache_settings.loadFromConfig(
            config,
            config_prefix,
            cache_path_prefix_if_relative,
            /* default_cache_path */"");

    if (file_cache_settings.isPathRelativeInConfig())
    {
        chassert(!cache_path_prefix_if_relative.empty());
        if (!is_attach && !pathStartsWith(file_cache_settings[FileCacheSetting::path].value, cache_path_prefix_if_relative))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Filesystem cache relative path must lie inside `{}`, but have {}",
                cache_path_prefix_if_relative, file_cache_settings[FileCacheSetting::path].value);
        }
    }
    else if (!cache_path_prefix_if_absolute.empty())
    {
        if (!is_attach && !pathStartsWith(file_cache_settings[FileCacheSetting::path].value, cache_path_prefix_if_absolute))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Filesystem cache absolute path must lie inside `{}`, but have {}",
                cache_path_prefix_if_absolute, file_cache_settings[FileCacheSetting::path].value);
        }
    }

    auto cache = FileCacheFactory::instance().getOrCreate(
        cache_name,
        file_cache_settings,
        predefined_configuration ? "" : config_prefix);

    return std::pair(cache, file_cache_settings);
}

void registerDiskCache(DiskFactory & factory, bool /* global_skip_access_check */)
{
    auto creator = [](const String & name,
                    const Poco::Util::AbstractConfiguration & config,
                    const String & config_prefix,
                    ContextPtr context,
                    const DisksMap & map,
                    bool attach,
                    bool custom_disk) -> DiskPtr
{
        auto disk_name = config.getString(config_prefix + ".disk", "");
        if (disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk Cache requires `disk` field in config");

        auto disk_it = map.find(disk_name);
        if (disk_it == map.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with cache layer `{}`: there is no such disk (it should be initialized before cache disk)",
                disk_name, name);
        }

        auto [cache, cache_settings] = getCache(config, config_prefix, context, name, attach, custom_disk);
        auto disk = disk_it->second;
        if (!dynamic_cast<const DiskObjectStorage *>(disk.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with cache layer `{}`: cached disk is allowed only on top of object storage",
                disk_name, name);

        auto disk_object_storage = disk->createDiskObjectStorage();
        disk_object_storage->wrapWithCache(cache, cache_settings, name);

        LOG_INFO(
            getLogger("DiskCache"),
            "Registered cached disk (`{}`) with structure: {}",
            name, assert_cast<DiskObjectStorage *>(disk_object_storage.get())->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("cache", creator);
}

}
