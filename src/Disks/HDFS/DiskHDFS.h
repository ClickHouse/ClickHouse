#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDiskRemote.h>
#include <Poco/DirectoryIterator.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <hdfs/hdfs.h> // Y_IGNORE
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
        const Poco::Util::AbstractConfiguration & config);

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

    RemoteDiskPtr getDiskPtr() override { return std::static_pointer_cast<DiskHDFS>(shared_from_this()); }

private:
    String getRandomName() { return toString(UUIDHelpers::generateV4()); }

    const Poco::Util::AbstractConfiguration & config;
    HDFSBuilderWrapper hdfs_builder;
    HDFSFSPtr hdfs_fs;

    std::mutex copying_mutex;
};

using DiskHDFSPtr = std::shared_ptr<DiskHDFS>;

}
