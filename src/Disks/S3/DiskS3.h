#pragma once

#include <atomic>
#include "Disks/DiskFactory.h"
#include "Disks/Executor.h"
#include "ProxyConfiguration.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>

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
    /// File contains restore information
    const String restore_file = "restore";

    using ObjectMetadata = std::map<std::string, std::string>;

    friend class DiskS3Reservation;

    class AwsS3KeyKeeper;
    struct Metadata;
    struct RestoreInformation;

    DiskS3(
        String name_,
        std::shared_ptr<Aws::S3::S3Client> client_,
        std::shared_ptr<S3::ProxyConfiguration> proxy_configuration_,
        String bucket_,
        String s3_root_path_,
        String metadata_path_,
        size_t min_upload_part_size_,
        size_t max_single_part_upload_size_,
        size_t min_bytes_for_seek_,
        bool send_metadata_);

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

    int open(const String & path, mode_t mode) const override;
    void close(int fd) const override;
    void sync(int fd) const override;

    const String getType() const override { return "s3"; }

    void shutdown() override;

    /// Actions performed after disk creation.
    void startup();

    /// Restore S3 metadata files on file system.
    void restore();

private:
    bool tryReserve(UInt64 bytes);

    void removeMeta(const String & path, AwsS3KeyKeeper & keys);
    void removeMetaRecursive(const String & path, AwsS3KeyKeeper & keys);
    void removeAws(const AwsS3KeyKeeper & keys);

    Metadata readMeta(const String & path) const;
    Metadata createMeta(const String & path) const;

    void createFileOperationObject(const String & operation_name, UInt64 revision, const ObjectMetadata & metadata);
    String revisionToString(UInt64 revision);
    bool checkObjectExists(const String & prefix);

    Aws::S3::Model::HeadObjectResult headObject(const String & source_bucket, const String & key);
    void listObjects(const String & source_bucket, const String & source_path, std::function<bool(const Aws::S3::Model::ListObjectsV2Result &)> callback);
    void restoreFiles(const String & source_bucket, const String & source_path, UInt64 revision);
    void processRestoreFiles(const String & source_bucket, std::vector<String> keys);
    void restoreFileOperations(const String & source_bucket, const String & source_path, UInt64 revision);
    UInt64 extractRevisionFromKey(const String & key);
    String extractOperationFromKey(const String & key);

private:
    const String name;
    std::shared_ptr<Aws::S3::S3Client> client;
    std::shared_ptr<S3::ProxyConfiguration> proxy_configuration;
    const String bucket;
    const String s3_root_path;
    const String metadata_path;
    size_t min_upload_part_size;
    size_t max_single_part_upload_size;
    size_t min_bytes_for_seek;
    bool send_metadata;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;

    std::atomic<UInt64> revision_counter;
};

}
