#pragma once

#include <Disks/DiskFactory.h>

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
class DiskHDFS : public IDisk, WithContext
{

friend class DiskHDFSReservation;

public:

    DiskHDFS(
        const String & name_,
        const String & hdfs_name_,
        const String & metadata_path_,
        ContextPtr context_);

    DiskType::Type getType() const override { return DiskType::Type::HDFS; }

    const String & getName() const override { return name; }

    const String & getPath() const override { return metadata_path; }

    UInt64 getTotalSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getAvailableSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getUnreservedSpace() const override { return std::numeric_limits<UInt64>::max(); }

    UInt64 getKeepingFreeSpace() const override { return 0; }

    bool exists(const String & path) const override;

    bool isFile(const String & path) const override;

    bool isDirectory(const String & path) const override;

    size_t getFileSize(const String & path) const override;

    void createDirectory(const String & path) override;

    void createDirectories(const String & path) override;

    void clearDirectory(const String & path) override;

    void moveDirectory(const String & from_path, const String & to_path) override { moveFile(from_path, to_path); }

    DiskDirectoryIteratorPtr iterateDirectory(const String & path) override;

    void createFile(const String & path) override;

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    void removeFile(const String & path) override;

    void removeFileIfExists(const String & path) override;

    void removeDirectory(const String & path) override;

    void removeRecursive(const String & path) override;

    ReservationPtr reserve(UInt64 bytes) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void setReadOnly(const String & path) override;

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

    bool tryReserve(UInt64 bytes);

    void remove(const String & path);

    Poco::Logger * log;
    const String name;
    const String hdfs_name;
    String metadata_path;
    const Poco::Util::AbstractConfiguration & config;

    HDFSBuilderWrapper builder;
    HDFSFSPtr fs;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;
    std::mutex copying_mutex;
};

using DiskHDFSPtr = std::shared_ptr<DiskHDFS>;

}
