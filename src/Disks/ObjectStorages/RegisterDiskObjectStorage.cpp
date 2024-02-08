#include <Disks/registerDisks.h>
#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include "DiskObjectStorageVFS.h"

namespace DB
{
void registerObjectStorages();
void registerMetadataStorages();

static std::string getCompatibilityMetadataTypeHint(const ObjectStorageType & type)
{
    switch (type)
    {
        case ObjectStorageType::S3:
        case ObjectStorageType::HDFS:
        case ObjectStorageType::Local:
        case ObjectStorageType::Azure:
            return "local";
        case ObjectStorageType::S3_Plain:
            return "plain";
        case ObjectStorageType::Web:
            return "web";
        case ObjectStorageType::None:
            return "";
    }
    UNREACHABLE();
}

void registerDiskObjectStorage(DiskFactory & factory, DiskStartupFlags disk_flags)
{
    registerObjectStorages();
    registerMetadataStorages();

    auto creator = [disk_flags](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /* map */,
        bool, bool) -> DiskPtr
    {
        bool skip_access_check = is_set(disk_flags, DiskStartupFlags::GLOBAL_SKIP_ACCESS_CHECK)
            || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        std::string compatibility_metadata_type_hint;
        if (!config.has(config_prefix + ".metadata_type"))
        {
            compatibility_metadata_type_hint = getCompatibilityMetadataTypeHint(object_storage->getType());
        }

        auto metadata_storage = MetadataStorageFactory::instance().create(
            name, config, config_prefix, object_storage, compatibility_metadata_type_hint);

// Keeper requires a disk to start up. A VFS disk requires a running Keeper in order to do an access check.
// This creates a circular dependency, therefore, VFS disks are prohibited.
#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
        if (is_set(disk_flags, DiskStartupFlags::ALLOW_VFS)
            && config.getBool(config_prefix + ".allow_vfs", false))
        {
            auto disk = std::make_shared<DiskObjectStorageVFS>(
                name,
                object_storage->getCommonKeyPrefix(),
                std::move(metadata_storage),
                std::move(object_storage),
                config,
                config_prefix,
                is_set(disk_flags, DiskStartupFlags::ALLOW_VFS_GC));
            disk->startup(context, skip_access_check);
            return disk;
        }
#endif

        DiskObjectStoragePtr disk = std::make_shared<DiskObjectStorage>(
            name,
            object_storage->getCommonKeyPrefix(),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix);

        disk->startup(context, skip_access_check);
        return disk;
    };

    factory.registerDiskType("object_storage", creator);
#if USE_AWS_S3
    factory.registerDiskType("s3", creator); /// For compatibility
    factory.registerDiskType("s3_plain", creator); /// For compatibility
#endif
#if USE_HDFS
    factory.registerDiskType("hdfs", creator); /// For compatibility
#endif
#if USE_AZURE_BLOB_STORAGE
    factory.registerDiskType("azure_blob_storage", creator); /// For compatibility
#endif
    factory.registerDiskType("local_blob_storage", creator); /// For compatibility
    factory.registerDiskType("web", creator); /// For compatibility
}

}
