#pragma once

#include <Disks/IDiskRemote.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Core/UUID.h>
#include <memory>


namespace DB
{

struct DiskHDFSSettings
{
    size_t min_bytes_for_seek;
    int thread_pool_size;
    int objects_chunk_size_to_delete;

    DiskHDFSSettings(
            int min_bytes_for_seek_,
            int thread_pool_size_,
            int objects_chunk_size_to_delete_)
        : min_bytes_for_seek(min_bytes_for_seek_)
        , thread_pool_size(thread_pool_size_)
        , objects_chunk_size_to_delete(objects_chunk_size_to_delete_) {}
};


/**
 * Storage for persisting data in HDFS and metadata on the local disk.
 * Files are represented by file in local filesystem (clickhouse_root/disks/disk_name/path/to/file)
 * that contains HDFS object key with actual data.
 */
class DiskHDFS final : public IDiskRemote
{
public:
    using SettingsPtr = std::unique_ptr<DiskHDFSSettings>;

    DiskHDFS(
        const String & disk_name_,
        const String & hdfs_root_path_,
        SettingsPtr settings_,
        const String & metadata_path_,
        const Poco::Util::AbstractConfiguration & config_);

    DiskType getType() const override { return DiskType::HDFS; }
    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return true; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> size) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;

    void removeFromRemoteFS(RemoteFSPathKeeperPtr fs_paths_keeper) override;

    RemoteFSPathKeeperPtr createFSPathKeeper() const override;

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote disk
    /// Required for remote disk to ensure that replica has access to data written by other node
    bool checkUniqueId(const String & hdfs_uri) const override;

private:
    String getRandomName() { return toString(UUIDHelpers::generateV4()); }

    const Poco::Util::AbstractConfiguration & config;

    HDFSBuilderWrapper hdfs_builder;
    HDFSFSPtr hdfs_fs;

    SettingsPtr settings;
};

}
