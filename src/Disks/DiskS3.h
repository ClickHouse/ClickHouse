#pragma once

#include <Common/config.h>

#if USE_AWS_S3
#    include "DiskFactory.h"

#    include <aws/s3/S3Client.h>
#    include <Poco/DirectoryIterator.h>


namespace DB
{
/**
 * Storage for persisting data in S3 and metadata on the local disk.
 * Files are represented by file in local filesystem (clickhouse_root/disks/disk_name/path/to/file)
 * that contains S3 object key with actual data.
 */
class DiskS3 : public IDisk
{
public:
    friend class DiskS3Reservation;

    DiskS3(
        String name_,
        std::shared_ptr<Aws::S3::S3Client> client_,
        String bucket_,
        String s3_root_path_,
        String metadata_path_,
        size_t min_upload_part_size_);

    const String & getName() const override { return name; }

    const String & getPath() const override { return s3_root_path; }

    ReservationPtr reserve(UInt64 bytes) override;

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

    void moveFile(const String & from_path, const String & to_path) override;

    void replaceFile(const String & from_path, const String & to_path) override;

    void copyFile(const String & from_path, const String & to_path) override;

    void listFiles(const String & path, std::vector<String> & file_names) override;

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        size_t estimated_size = 0,
        size_t aio_threshold = 0,
        size_t mmap_threshold = 0) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        size_t estimated_size = 0,
        size_t aio_threshold = 0) override;

    void remove(const String & path) override;

    void removeRecursive(const String & path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

private:
    String getRandomName() const;

    bool tryReserve(UInt64 bytes);

private:
    const String name;
    std::shared_ptr<Aws::S3::S3Client> client;
    const String bucket;
    const String s3_root_path;
    const String metadata_path;
    size_t min_upload_part_size;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;
};

}

#endif
