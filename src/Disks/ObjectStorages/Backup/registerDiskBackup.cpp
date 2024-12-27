#include <Common/logger_useful.h>
#include <Common/assert_cast.h>
#include <Disks/DiskFactory.h>
#include <Disks/ObjectStorages/Backup/BackupObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskBackup(DiskFactory & factory, bool /* global_skip_access_check */)
{
    auto creator = [](const String & name,
                    const Poco::Util::AbstractConfiguration & config,
                    const String & config_prefix,
                    ContextPtr context,
                    const DisksMap & map,
                    bool ,bool) -> DiskPtr
    {
        auto disk_name = config.getString(config_prefix + ".disk", "");
        if (disk_name.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk Backup requires `disk` field in config");

        auto disk_it = map.find(disk_name);
        if (disk_it == map.end())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot wrap disk `{}` with backup layer `{}`: there is no such disk (it should be initialized before backup disk)",
                disk_name, name);
        }

        auto backup_base_path = config.getString(config_prefix + ".path", fs::path(context->getPath()) / "disks" / name / "backup/");
        if (!fs::exists(backup_base_path))
            fs::create_directories(backup_base_path);

        auto disk = disk_it->second;
        auto disk_object_storage = disk->createDiskObjectStorage();
        disk_object_storage->wrapWithBackup(name, backup_base_path);

        LOG_INFO(
            &Poco::Logger::get("DiskBackup"),
            "Registered backup disk (`{}`) with structure: {}",
            name, assert_cast<DiskObjectStorage *>(disk_object_storage.get())->getStructure());

        return disk_object_storage;
    };

    factory.registerDiskType("backup", creator);
}

}
