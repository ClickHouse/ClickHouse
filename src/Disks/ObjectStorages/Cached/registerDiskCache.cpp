#include <Common/FileCacheSettings.h>
#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskCache(DiskFactory & factory)
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
                "There is not disk with name `{}`, disk name should be initialized before cache disk",
                disk_name);
        }

        FileCacheSettings file_cache_settings;
        file_cache_settings.loadFromConfig(config, config_prefix);

        auto cache_base_path = config.getString(config_prefix + ".path", fs::path(context->getPath()) / "disks" / name / "cache/");
        if (!fs::exists(cache_base_path))
            fs::create_directories(cache_base_path);

        auto disk = disk_it->second;
        auto object_storage = disk->getObjectStorage(disk_name);

        auto cache = FileCacheFactory::instance().getOrCreate(cache_base_path, file_cache_settings);
        object_storage->wrapWithCache(cache, name);

        return object_storage;
    };

    factory.registerDiskType("cache", creator);
}

}
