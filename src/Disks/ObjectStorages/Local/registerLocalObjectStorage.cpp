#include <Disks/DiskFactory.h>
#include <Disks/loadLocalDiskConfig.h>
#include <Disks/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
void registerDiskLocalObjectStorage(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        String object_key_prefix;
        UInt64 keep_free_space_bytes;
        loadDiskLocalConfig(name, config, config_prefix, context, object_key_prefix, keep_free_space_bytes);
        /// keys are mapped to the fs, object_key_prefix is a directory also
        fs::create_directories(object_key_prefix);

        String type = config.getString(config_prefix + ".type");
        chassert(type == "local_blob_storage");

        std::shared_ptr<LocalObjectStorage> local_storage = std::make_shared<LocalObjectStorage>(object_key_prefix);
        MetadataStoragePtr metadata_storage;
        auto [metadata_path, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);
        metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, object_key_prefix);

        auto disk = std::make_shared<DiskObjectStorage>(
            name, object_key_prefix, "Local", metadata_storage, local_storage, config, config_prefix);
        disk->startup(context, global_skip_access_check);
        return disk;

    };
    factory.registerDiskType("local_blob_storage", creator);
}

}
