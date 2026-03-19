#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.h>
#include <Disks/DiskObjectStorage/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/ReadOnlyDiskWrapper.h>
#include <Disks/DiskFactory.h>
#include <Disks/IDisk.h>

#include <fmt/ranges.h>

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
        const bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        std::unordered_map<Location, ObjectStoragePtr> object_storage_registry;
        std::unordered_map<Location, LocationInfo> cluster_registry;
        if (config.has(config_prefix + ".locations"))
        {
            Locations locations;
            config.keys(config_prefix + ".locations", locations);
            LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Configuring DiskObjectStorage with multiple locations: [{}]", fmt::join(locations, ", "));

            for (const auto & location : locations)
            {
                const std::string object_storage_config_prefix = config_prefix + ".locations." + location;
                const bool local = config.getBool(object_storage_config_prefix + ".local");
                const bool enabled = config.getBool(object_storage_config_prefix + ".enabled");
                const ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create(fmt::format("{}.{}", name, location), config, object_storage_config_prefix, context, /*skip_access_check=*/skip_access_check || !enabled);
                object_storage_registry[location] = object_storage;
                cluster_registry[location] = {enabled, local, object_storage_config_prefix};
            }
        }
        else
        {
            const ObjectStoragePtr object_storage = ObjectStorageFactory::instance().create(name, config, config_prefix, context, skip_access_check);
            object_storage_registry["main"] = object_storage;
            cluster_registry["main"] = { .enabled = true, .local = true, .config_prefix = config_prefix };
        }

        ClusterConfigurationPtr cluster = std::make_shared<ClusterConfiguration>(std::move(cluster_registry));
        ObjectStorageRouterPtr object_storages = std::make_shared<ObjectStorageRouter>(std::move(object_storage_registry));

        std::string compatibility_metadata_type_hint;
        if (!config.has(config_prefix + ".metadata_type"))
        {
            auto type = config.getString(config_prefix + ".type", "");

            if (type.contains("with_keeper"))
                compatibility_metadata_type_hint = "keeper";
            else if (type.contains("plain") && type.contains("rewritable"))
                compatibility_metadata_type_hint = "plain_rewritable";
            else if (type.contains("plain"))
                compatibility_metadata_type_hint = "plain";
            else
                compatibility_metadata_type_hint = MetadataStorageFactory::getCompatibilityMetadataTypeHint(cluster, object_storages);
        }

        LOG_DEBUG(getLogger("registerDiskObjectStorage"), "Metadata type hint: {}", compatibility_metadata_type_hint);
        auto metadata_storage = MetadataStorageFactory::instance().create(name, config, config_prefix, cluster, object_storages, compatibility_metadata_type_hint);

        bool use_fake_transaction = config.getBool(config_prefix + ".use_fake_transaction", metadata_storage->getType() != MetadataStorageType::Keeper);
        DiskPtr disk = std::make_shared<DiskObjectStorage>(
            name,
            std::move(cluster),
            std::move(metadata_storage),
            std::move(object_storages),
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
