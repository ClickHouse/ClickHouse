#include <Common/FileCacheSettings.h>
#include <Common/FileCacheFactory.h>
#include <Common/IFileCache.h>
#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/Cached/CachedObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Interpreters/Context.h>

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
                "Cannot wrap disk `{}` with cache layer `{}`: there is no such disk (it should be initialized before cache disk)",
                disk_name, name);
        }

        FileCacheSettings file_cache_settings;
        file_cache_settings.loadFromConfig(config, config_prefix);

        auto cache_base_path = config.getString(config_prefix + ".path", fs::path(context->getPath()) / "disks" / name / "cache/");
        if (!fs::exists(cache_base_path))
            fs::create_directories(cache_base_path);

        auto disk = disk_it->second;

        auto cache = FileCacheFactory::instance().getOrCreate(cache_base_path, file_cache_settings, name);
        auto disk_object_storage = disk->createDiskObjectStorage();

        disk_object_storage->wrapWithCache(cache, name);

        LOG_INFO(
            &Poco::Logger::get("DiskCache"),
            "Registered cached disk (`{}`) with structure: {}",
            name, assert_cast<DiskObjectStorage *>(disk_object_storage.get())->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("cache", creator);
}

}
