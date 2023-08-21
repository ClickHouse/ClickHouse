#include <Disks/ObjectStorages/NFS/NFSObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/DiskFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int CANNOT_UNLINK;
}

void loadDiskNFSConfig(const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      String & path)
{
    path = config.getString(config_prefix + ".path", "");
    if (path.empty())
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path can not be empty. Disk {}", name);
    if (path.back() != '/')
        throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Disk path must end with /. Disk {}", name);
}

std::unique_ptr<NFSObjectStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return std::make_unique<NFSObjectStorageSettings>(
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        config.getUInt64(config_prefix + ".nfs_max_single_read_retries", 1));
}

void registerDiskNFS(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & map) -> DiskPtr
    {
        Poco::Logger * log = &Poco::Logger::get("DiskNFS");

        //The path to the real remote disk
        String root_path;
        loadDiskNFSConfig(name, config, config_prefix, root_path);

        for (const auto & [disk_name, disk_ptr] : map)
            if (root_path == disk_ptr->getPath())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, root_path);

        auto settings = getSettings(config, config_prefix);

        auto nfs_storage = std::make_shared<NFSObjectStorage>(name, root_path, context, std::move(settings), config);

        auto [_, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);
        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, root_path);
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        LOG_TRACE(log, "Disk name {}, root path {}, meta disk path {}", name, root_path, metadata_disk->getPath());

        auto disk = std::make_shared<DiskObjectStorage>(
            name,
            root_path,
            "DiskNFS",
            std::move(metadata_storage),
            std::move(nfs_storage),
            config,
            config_prefix);

        disk->startup(context, skip_access_check);

        return disk;
    };
    factory.registerDiskType("nfs", creator);
}

}
