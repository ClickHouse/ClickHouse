#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include "DiskObjectStorageVFS.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

void registerObjectStorages();
void registerMetadataStorages();

static std::string getCompatibilityMetadataTypeHint(const DataSourceDescription & description)
{
    switch (description.type)
    {
        case DataSourceType::S3:
        case DataSourceType::HDFS:
        case DataSourceType::LocalBlobStorage:
        case DataSourceType::AzureBlobStorage:
            return "local";
        case DataSourceType::S3_Plain:
            return "plain";
        case DataSourceType::WebServer:
            return "web";
        default:
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
                            "Cannot get compatibility metadata hint: "
                            "no such object storage type: {}", toString(description.type));
    }
    UNREACHABLE();
}

void registerDiskObjectStorage(DiskFactory & factory, bool global_skip_access_check)
{
    registerObjectStorages();
    registerMetadataStorages();

    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        auto compatibility_metadata_type_hint = config.has("metadata_type")
            ? ""
            : getCompatibilityMetadataTypeHint(object_storage->getDataSourceDescription());

        auto metadata_storage = MetadataStorageFactory::instance().create(
            name, config, config_prefix, object_storage, compatibility_metadata_type_hint);

        const DataSourceType type = object_storage->getDataSourceDescription().type;

// Keeper requires a disk to start up. A VFS disk requires a running Keeper in order to do an access check.
// This creates a circular dependency, therefore, VFS disks are prohibited.
#ifndef CLICKHOUSE_KEEPER_STANDALONE_BUILD
        if (config.getBool(config_prefix + ".allow_vfs", false))
        {
            using enum DataSourceType;
            if (type != S3)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "allow_vfs supports only 's3' disk types");

            const bool allow_vfs_gc = true; // TODO myrrc re-add GC ban for Keeper and ch-disks

            auto disk = std::make_shared<DiskObjectStorageVFS>(
                name,
                object_storage->getCommonKeyPrefix(),
                fmt::format("DiskVFS_{}({})", toString(type), name),
                std::move(metadata_storage),
                std::move(object_storage),
                config,
                config_prefix,
                allow_vfs_gc);

            disk->startup(context, skip_access_check);
            return disk;
        }
#endif

        DiskObjectStoragePtr disk = std::make_shared<DiskObjectStorage>(
            name,
            object_storage->getCommonKeyPrefix(),
            fmt::format("Disk_{}({})", toString(type), name),
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
