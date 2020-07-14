#pragma once

#include "Disks/DiskFactory.h"
#include "ProxyConfiguration.h"

#include <aws/s3/S3Client.h>
#include <Poco/DirectoryIterator.h>


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
        std::shared_ptr<S3::ProxyConfiguration> proxy_configuration_,
        String bucket_,
        String s3_root_path_,
        String metadata_path_,
        size_t min_upload_part_size_,
        size_t min_multi_part_upload_size_,
        size_t min_bytes_for_seek_);

    const String & getName() const override { return name; }

    const String & getPath() const override { return metadata_path; }

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
        size_t buf_size,
        size_t estimated_size,
        size_t aio_threshold,
        size_t mmap_threshold) const override;

    std::unique_ptr<WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        WriteMode mode,
        size_t estimated_size,
        size_t aio_threshold) override;

    void remove(const String & path) override;

    void removeRecursive(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void createFile(const String & path) override;

    void setReadOnly(const String & path) override;

    const String getType() const override { return "s3"; }

private:
    bool tryReserve(UInt64 bytes);

private:
    const String name;
    std::shared_ptr<Aws::S3::S3Client> client;
    std::shared_ptr<S3::ProxyConfiguration> proxy_configuration;
    const String bucket;
    const String s3_root_path;
    const String metadata_path;
    size_t min_upload_part_size;
    size_t min_multi_part_upload_size;
    size_t min_bytes_for_seek;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;
};

}
