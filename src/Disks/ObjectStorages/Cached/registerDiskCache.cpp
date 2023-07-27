#include <Interpreters/Cache/FileCacheSettings.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
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
        file_cache_settings.loadFromConfig(config, config_prefix);

        if (file_cache_settings.base_path.empty())
            file_cache_settings.base_path = fs::path(context->getPath()) / "disks" / name / "cache/";
        else if (fs::path(file_cache_settings.base_path).is_relative())
            file_cache_settings.base_path = fs::path(context->getPath()) / "caches" / file_cache_settings.base_path;

        auto cache = FileCacheFactory::instance().getOrCreate(name, file_cache_settings);
        auto disk = disk_it->second;
        auto disk_object_storage = disk->createDiskObjectStorage();

        disk_object_storage->wrapWithCache(cache, file_cache_settings, name);

        LOG_INFO(
            &Poco::Logger::get("DiskCache"),
            "Registered cached disk (`{}`) with structure: {}",
            name, assert_cast<DiskObjectStorage *>(disk_object_storage.get())->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("cache", creator);
}

}
