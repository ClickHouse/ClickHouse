#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/DiskFactory.h>
#include <Disks/DiskRestartProxy.h>
#include <Storages/HDFS/HDFSCommon.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskHDFS(DiskFactory & factory)
{
    auto creator = [](const String & name,
                      const Poco::Util::AbstractConfiguration & config,
                      const String & config_prefix,
                      ContextPtr context_,
                      const DisksMap & /*map*/) -> DiskPtr
    {
        String uri{config.getString(config_prefix + ".endpoint")};
        checkHDFSURL(uri);

        if (uri.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

        std::unique_ptr<HDFSObjectStorageSettings> settings = std::make_unique<HDFSObjectStorageSettings>(
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
            context_->getSettingsRef().hdfs_replication
        );


        /// FIXME Cache currently unsupported :(
        ObjectStoragePtr hdfs_storage = std::make_unique<HDFSObjectStorage>(uri, std::move(settings), config);

        auto [_, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context_);

        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, uri);
        uint64_t copy_thread_pool_size = config.getUInt(config_prefix + ".thread_pool_size", 16);

        DiskPtr disk_result = std::make_shared<DiskObjectStorage>(
            name,
            uri,
            "DiskHDFS",
            std::move(metadata_storage),
            std::move(hdfs_storage),
            /* send_metadata = */ false,
            copy_thread_pool_size);

        return std::make_shared<DiskRestartProxy>(disk_result);
    };

    factory.registerDiskType("hdfs", creator);
}

}
