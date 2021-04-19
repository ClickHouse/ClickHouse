#pragma once

#include <Disks/DiskFactory.h>
#include <Disks/IDiskRemote.h>

#include <Poco/DirectoryIterator.h>
#include <Storages/HDFS/HDFSCommon.h>
#include <hdfs/hdfs.h> // Y_IGNORE
#include <Core/UUID.h>


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
        ContextPtr context_);

    DiskType::Type getType() const override { return DiskType::Type::HDFS; }

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    void moveFile(const String & from_path, const String & to_path) override;

    void removeFile(const String & path) override;

    void removeFileIfExists(const String & path) override;

    void removeRecursive(const String & path) override;

    ReservationPtr reserve(UInt64 bytes) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold,
        MMappedFileCache * mmap_cache) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode) override;

private:
    String getRandomName() { return toString(UUIDHelpers::generateV4()); }

    void remove(const String & path);

    const Poco::Util::AbstractConfiguration & config;
    String hdfs_root_path;
    HDFSBuilderWrapper hdfs_builder;
    HDFSFSPtr hdfs_fs;

    std::mutex copying_mutex;
};

using DiskHDFSPtr = std::shared_ptr<DiskHDFS>;

}
