#include <Disks/ObjectStorages/HDFS/HDFSObjectStorage.h>
#include <Disks/ObjectStorages/DiskObjectStorageCommon.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <Disks/DiskFactory.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Interpreters/Context.h>
#include <Common/Macros.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerDiskHDFS(DiskFactory & factory, bool global_skip_access_check)
{
    auto creator = [global_skip_access_check](
        const String & name,
        const Poco::Util::AbstractConfiguration & config,
        const String & config_prefix,
        ContextPtr context,
        const DisksMap & /*map*/) -> DiskPtr
    {
        String endpoint = context->getMacros()->expand(config.getString(config_prefix + ".endpoint"));
        String uri{endpoint};
        checkHDFSURL(uri);

        if (uri.back() != '/')
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "HDFS path must ends with '/', but '{}' doesn't.", uri);

        std::unique_ptr<HDFSObjectStorageSettings> settings = std::make_unique<HDFSObjectStorageSettings>(
            config.getUInt64(config_prefix + ".min_bytes_for_seek", 1024 * 1024),
            config.getInt(config_prefix + ".objects_chunk_size_to_delete", 1000),
            context->getSettingsRef().hdfs_replication
        );


        /// FIXME Cache currently unsupported :(
        ObjectStoragePtr hdfs_storage = std::make_unique<HDFSObjectStorage>(uri, std::move(settings), config);

        auto [_, metadata_disk] = prepareForLocalMetadata(name, config, config_prefix, context);

        auto metadata_storage = std::make_shared<MetadataStorageFromDisk>(metadata_disk, uri);
        bool skip_access_check = global_skip_access_check || config.getBool(config_prefix + ".skip_access_check", false);

        DiskPtr disk = std::make_shared<DiskObjectStorage>(
            name,
            uri,
            "DiskHDFS",
            std::move(metadata_storage),
            std::move(hdfs_storage),
            config,
            config_prefix);
        disk->startup(context, skip_access_check);

        return disk;
    };

    factory.registerDiskType("hdfs", creator);
}

}
