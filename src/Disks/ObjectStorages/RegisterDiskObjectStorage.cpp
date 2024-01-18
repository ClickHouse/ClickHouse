#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>

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
        std::string compatibility_metadata_type_hint;
        if (!config.has("metadata_type"))
        {
            compatibility_metadata_type_hint = getCompatibilityMetadataTypeHint(object_storage->getType());
        }

        auto metadata_storage = MetadataStorageFactory::instance().create(
            name, config, config_prefix, object_storage, compatibility_metadata_type_hint);

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
