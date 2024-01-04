#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Common/filesystemHelpers.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskCache(DiskFactory & factory, bool /* global_skip_access_check */)
{
    auto creator = [](const String & name,
                    const Poco::Util::AbstractConfiguration & config,
                    const String & config_prefix,
                    ContextPtr context,
                    const DisksMap & map) -> DiskPtr
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

        FileCacheSettings file_cache_settings;
        auto predefined_configuration = config.has("cache_name") ? NamedCollectionFactory::instance().tryGet(config.getString("cache_name")) : nullptr;
        if (predefined_configuration)
            file_cache_settings.loadFromCollection(*predefined_configuration);
        else
            file_cache_settings.loadFromConfig(config, config_prefix);

        auto config_fs_caches_dir = context->getFilesystemCachesPath();
        if (config_fs_caches_dir.empty())
        {
            if (fs::path(file_cache_settings.base_path).is_relative())
                file_cache_settings.base_path = fs::path(context->getPath()) / "caches" / file_cache_settings.base_path;
        }
        else
        {
            if (fs::path(file_cache_settings.base_path).is_relative())
                file_cache_settings.base_path = fs::path(config_fs_caches_dir) / file_cache_settings.base_path;

            if (!pathStartsWith(file_cache_settings.base_path, config_fs_caches_dir))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Filesystem cache path {} must lie inside default filesystem cache path `{}`",
                                file_cache_settings.base_path, config_fs_caches_dir);
            }
        }

        auto cache = FileCacheFactory::instance().getOrCreate(name, file_cache_settings, predefined_configuration ? "" : config_prefix);
        auto disk = disk_it->second;
        if (!dynamic_cast<const DiskObjectStorage *>(disk.get()))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with cache layer `{}`: cached disk is allowed only on top of object storage",
                disk_name, name);

        auto disk_object_storage = disk->createDiskObjectStorage();

        disk_object_storage->wrapWithCache(cache, file_cache_settings, name);

        LOG_INFO(
            &Poco::Logger::get("DiskCache"),
            "Registered cached disk (`{}`) with structure: {}",
            // assert_cast wouldn't work for classes deriving from DiskObjectStorage
            name, disk_object_storage->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("cache", creator);
}

}
