#include <Disks/DiskFactory.h>
#include <Interpreters/Context.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFactory.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/ReadOnlyDiskWrapper.h>

namespace DB
{

void registerObjectStorages();
void registerMetadataStorages();

void registerDiskObjectStorage(DiskFactory & factory, bool global_skip_access_check)
{
    registerObjectStorages();
    registerMetadataStorages();

    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /* map */,
        bool, bool) -> DiskPtr
    {
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);
        auto object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);

        std::string compatibility_metadata_type_hint;
        if (!config.has(config_prefix + ".metadata_type"))
        {
            if (config.getString(config_prefix + ".type", "") == "s3_with_keeper")
                compatibility_metadata_type_hint = "keeper";
            else if (object_storage->isPlain())
                if (object_storage->isWriteOnce())
                    compatibility_metadata_type_hint = "plain";
                else
                    compatibility_metadata_type_hint = "plain_rewritable";
            else
                compatibility_metadata_type_hint = MetadataStorageFactory::getCompatibilityMetadataTypeHint(object_storage->getType());
        }

        auto metadata_storage = MetadataStorageFactory::instance().create(
            name, config, config_prefix, object_storage, compatibility_metadata_type_hint);

        bool use_fake_transaction = config.getBool(config_prefix + ".use_fake_transaction", metadata_storage->getType() != MetadataStorageType::Keeper);

        DiskPtr disk = std::make_shared<DiskObjectStorage>(
            name,
            object_storage->getCommonKeyPrefix(),
            std::move(metadata_storage),
            std::move(object_storage),
            config,
            config_prefix,
            use_fake_transaction);

        /// If this disk was created "on the fly" in order to serve as a temporary read-only disk.
        bool is_read_only_disk = config.getBool(config_prefix + ".read_only", false);
        if (is_read_only_disk)
        {
            LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Using read-only disk wrapper");
            disk = std::make_shared<ReadOnlyDiskWrapper>(disk);
        }

        disk->startup(skip_access_check);
        return disk;
    };

    factory.registerDiskType("object_storage", creator);
#if USE_AWS_S3
    factory.registerDiskType("s3", creator); /// For compatibility
    factory.registerDiskType("s3_plain", creator); /// For compatibility
    factory.registerDiskType("s3_with_keeper", creator); /// For compatibility
    factory.registerDiskType("s3_plain_rewritable", creator); // For compatibility
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
