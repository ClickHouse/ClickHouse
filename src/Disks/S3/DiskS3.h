#pragma once

#include <atomic>
#include "Disks/DiskFactory.h"
#include "Disks/Executor.h"
#include "ProxyConfiguration.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>

#include <Poco/DirectoryIterator.h>
#include <re2/re2.h>


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
        UInt64 max_single_read_retries_,
        size_t min_upload_part_size_,
        size_t max_single_part_upload_size_,
        size_t min_bytes_for_seek_,
        bool send_metadata_,
        int thread_pool_size_,
        int list_object_keys_size_);

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
        WriteMode mode) override;

    void removeFile(const String & path) override;
    void removeFileIfExists(const String & path) override;
    void removeDirectory(const String & path) override;
    void removeRecursive(const String & path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;

    void setLastModified(const String & path, const Poco::Timestamp & timestamp) override;

    Poco::Timestamp getLastModified(const String & path) override;

    void createFile(const String & path) override;

    void setReadOnly(const String & path) override;

    DiskType::Type getType() const override { return DiskType::Type::S3; }

    void shutdown() override;

    /// Actions performed after disk creation.
    void startup();

    /// Restore S3 metadata files on file system.
    void restore();

    /// Dumps current revision counter into file 'revision.txt' at given path.
    void onFreeze(const String & path) override;

private:
    bool tryReserve(UInt64 bytes);

    void removeMeta(const String & path, AwsS3KeyKeeper & keys);
    void removeMetaRecursive(const String & path, AwsS3KeyKeeper & keys);
    void removeAws(const AwsS3KeyKeeper & keys);

    Metadata readMeta(const String & path) const;
    Metadata createMeta(const String & path) const;

    void createFileOperationObject(const String & operation_name, UInt64 revision, const ObjectMetadata & metadata);
    static String revisionToString(UInt64 revision);

    bool checkObjectExists(const String & prefix);
    Aws::S3::Model::HeadObjectResult headObject(const String & source_bucket, const String & key);
    void listObjects(const String & source_bucket, const String & source_path, std::function<bool(const Aws::S3::Model::ListObjectsV2Result &)> callback);
    void copyObject(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key);

    void readRestoreInformation(RestoreInformation & restore_information);
    void restoreFiles(const String & source_bucket, const String & source_path, UInt64 target_revision);
    void processRestoreFiles(const String & source_bucket, const String & source_path, std::vector<String> keys);
    void restoreFileOperations(const String & source_bucket, const String & source_path, UInt64 target_revision);

    /// Remove 'path' prefix from 'key' to get relative key.
    /// It's needed to store keys to metadata files in RELATIVE_PATHS version.
    static String shrinkKey(const String & path, const String & key);
    std::tuple<UInt64, String> extractRevisionAndOperationFromKey(const String & key);

    const String name;
    std::shared_ptr<Aws::S3::S3Client> client;
    std::shared_ptr<S3::ProxyConfiguration> proxy_configuration;
    const String bucket;
    const String s3_root_path;
    const String metadata_path;
    UInt64 max_single_read_retries;
    size_t min_upload_part_size;
    size_t max_single_part_upload_size;
    size_t min_bytes_for_seek;
    bool send_metadata;

    UInt64 reserved_bytes = 0;
    UInt64 reservation_count = 0;
    std::mutex reservation_mutex;

    std::atomic<UInt64> revision_counter;
    static constexpr UInt64 LATEST_REVISION = (static_cast<UInt64>(1)) << 63;
    static constexpr UInt64 UNKNOWN_REVISION = 0;

    /// File at path {metadata_path}/restore contains metadata restore information
    const String restore_file_name = "restore";
    /// The number of keys listed in one request (1000 is max value)
    int list_object_keys_size;

    /// Key has format: ../../r{revision}-{operation}
    const re2::RE2 key_regexp {".*/r(\\d+)-(\\w+).*"};
};

}
