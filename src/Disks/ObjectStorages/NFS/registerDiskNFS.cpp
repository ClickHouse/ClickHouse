#include <Disks/ObjectStorages/NFS/NFSObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskRestartProxy.h>

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
        throw Exception("Disk path can not be empty. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    if (path.back() != '/')
        throw Exception("Disk path must end with /. Disk " + name, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
}

std::unique_ptr<NFSObjectStorageSettings> getSettings(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    return std::make_unique<NFSObjectStorageSettings>(
        config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
        config.getUInt64(config_prefix + ".remote_file_buffer_size", 10 * 1024 * 1024),
        config.getInt(config_prefix + ".thread_pool_size", 16),
        config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
        config.getUInt64(config_prefix + ".nfs_max_single_read_retries", 3));
}

void registerDiskNFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context,
                      const DisksMap & map) -> DiskPtr
    {
        //The path to the real remote disk
        String root_path;
        Poco::Logger * log = &Poco::Logger::get("DiskNFS");

        loadDiskNFSConfig(name, config, config_prefix, root_path);

        for (const auto & [disk_name, disk_ptr] : map)
            if (root_path == disk_ptr->getPath())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Disk {} and disk {} cannot have the same path ({})", name, disk_name, root_path);

        auto settings = getSettings(config, config_prefix);

        ObjectStoragePtr nfs_storage = std::make_shared<NFSObjectStorage>(name, root_path, context, std::move(settings), config);

        auto [_, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);
        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, root_path);
        uint64_t copy_thread_pool_size = config.getUInt(config_prefix + ".thread_pool_size", 16);

        LOG_TRACE(log, "Disk name {}, root path {}, meta disk path {}", name, root_path, metadata_disk->getPath());

        DiskPtr disk_result = std::make_shared<DiskObjectStorage>(
            name,
            root_path,
            "DiskNFS",
            std::move(metadata_storage),
            std::move(nfs_storage),
            /* send_metadata = */ false,
            copy_thread_pool_size);

        return std::make_shared<DiskRestartProxy>(disk_result);
    };
    factory.registerDiskType("nfs", creator);
}

}

