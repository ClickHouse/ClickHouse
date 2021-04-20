#pragma once

#include <Disks/IDiskRemote.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <Core/UUID.h>
#include <memory>


namespace DB
{

/**
 * Storage for persisting data in HDFS and metadata on the local disk.
 * Files are represented by file in local filesystem (clickhouse_root/disks/disk_name/path/to/file)
 * that contains HDFS object key with actual data.
 */
class DiskHDFS : public IDiskRemote
{

friend class DiskHDFSReservation;

public:

    DiskHDFS(
        const String & disk_name_,
        const String & hdfs_root_path_,
        const String & metadata_path_,
        const Poco::Util::AbstractConfiguration & config_,
        size_t min_bytes_for_seek_);

    DiskType::Type getType() const override { return DiskType::Type::HDFS; }

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(const String & path, size_t buf_size, WriteMode mode) override;

    void removeFromRemoteFS(const Metadata & metadata) override;

private:
    String getRandomName() { return toString(UUIDHelpers::generateV4()); }

    const Poco::Util::AbstractConfiguration & config;
    size_t min_bytes_for_seek;

    HDFSBuilderWrapper hdfs_builder;
    HDFSFSPtr hdfs_fs;

    std::mutex copying_mutex;
};

}
