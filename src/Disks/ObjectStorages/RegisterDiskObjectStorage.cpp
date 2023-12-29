#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

static std::string getCompatibilityMetadataTypeHint(const std::string & object_storage_type)
{
    /// TODO: change type name to enum
    if (object_storage_type == "s3"
        || object_storage_type == "hdfs"
        || object_storage_type == "azure_blob_storage"
        || object_storage_type == "local_blob_storage")
        return "local";
    else if (object_storage_type == "s3_plain")
        return "plain";
    else if (object_storage_type == "web")
        return "web";
    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown object storage type: {}", object_storage_type);
}

void registerDiskObjectStorage(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
        auto compatibility_metadata_type_hint = config.has("metadata_type") ? "" : getCompatibilityMetadataTypeHint(object_storage->getTypeName());
        auto metadata_storage = MetadataStorageFactory::instance().create(name, config, config_prefix, object_storage, compatibility_metadata_type_hint);

        DiskObjectStoragePtr disk = std::make_shared<DiskObjectStorage>(
            name,
            object_storage->getBasePath(),
            "Disk" + object_storage->getTypeName(),
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
