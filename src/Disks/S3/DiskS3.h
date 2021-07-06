#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_AWS_S3

#include <atomic>
#include <common/logger_useful.h>
#include "Disks/DiskFactory.h"
#include "Disks/Executor.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>

#include <Poco/DirectoryIterator.h>
#include <re2/re2.h>
#include <Disks/IDiskRemote.h>


namespace DB
{

/// Settings for DiskS3 that can be changed in runtime.
struct DiskS3Settings
{
    DiskS3Settings(
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        size_t s3_max_single_read_retries_,
        size_t s3_min_upload_part_size_,
        size_t s3_max_single_part_upload_size_,
        size_t min_bytes_for_seek_,
        bool send_metadata_,
        int thread_pool_size_,
        int list_object_keys_size_,
        int objects_chunk_size_to_delete_);

    std::shared_ptr<Aws::S3::S3Client> client;
    size_t s3_max_single_read_retries;
    size_t s3_min_upload_part_size;
    size_t s3_max_single_part_upload_size;
    size_t min_bytes_for_seek;
    bool send_metadata;
    int thread_pool_size;
    int list_object_keys_size;
    int objects_chunk_size_to_delete;
};


/**
 * Storage for persisting data in S3 and metadata on the local disk.
 * Files are represented by file in local filesystem (clickhouse_root/disks/disk_name/path/to/file)
 * that contains S3 object key with actual data.
 */
class DiskS3 final : public IDiskRemote
{
public:
    using ObjectMetadata = std::map<std::string, std::string>;
    using Futures = std::vector<std::future<void>>;

    using SettingsPtr = std::unique_ptr<DiskS3Settings>;
    using GetDiskSettings = std::function<SettingsPtr(const Poco::Util::AbstractConfiguration &, const String, ContextPtr)>;

    struct RestoreInformation;

    DiskS3(
        String name_,
        String bucket_,
        String s3_root_path_,
        String metadata_path_,
        SettingsPtr settings_,
        GetDiskSettings settings_getter_);

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

    void removeFromRemoteFS(RemoteFSPathKeeperPtr keeper) override;

    RemoteFSPathKeeperPtr createFSPathKeeper() const override;

    void moveFile(const String & from_path, const String & to_path, bool send_metadata);
    void moveFile(const String & from_path, const String & to_path) override;

    void createHardLink(const String & src_path, const String & dst_path) override;
    void createHardLink(const String & src_path, const String & dst_path, bool send_metadata);

    DiskType::Type getType() const override { return DiskType::Type::S3; }

    void shutdown() override;

    void startup() override;

    /// Return some uniq string for file
    /// Required for distinguish different copies of the same part on S3
    String getUniqueId(const String & path) const override;

    /// Check file exists and ClickHouse has an access to it
    /// Required for S3 to ensure that replica has access to data wroten by other node
    bool checkUniqueId(const String & id) const override;

    /// Dumps current revision counter into file 'revision.txt' at given path.
    void onFreeze(const String & path) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context) override;

private:
    void createFileOperationObject(const String & operation_name, UInt64 revision, const ObjectMetadata & metadata);
    /// Converts revision to binary string with leading zeroes (64 bit).
    static String revisionToString(UInt64 revision);

    bool checkObjectExists(const String & source_bucket, const String & prefix) const;
    void findLastRevision();

    int readSchemaVersion(const String & source_bucket, const String & source_path);
    void saveSchemaVersion(const int & version);
    void updateObjectMetadata(const String & key, const ObjectMetadata & metadata);
    void migrateFileToRestorableSchema(const String & path);
    void migrateToRestorableSchemaRecursive(const String & path, Futures & results);
    void migrateToRestorableSchema();

    Aws::S3::Model::HeadObjectResult headObject(const String & source_bucket, const String & key) const;
    void listObjects(const String & source_bucket, const String & source_path, std::function<bool(const Aws::S3::Model::ListObjectsV2Result &)> callback) const;
    void copyObject(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key) const;

    /// Restore S3 metadata files on file system.
    void restore();
    void readRestoreInformation(RestoreInformation & restore_information);
    void restoreFiles(const RestoreInformation & restore_information);
    void processRestoreFiles(const String & source_bucket, const String & source_path, std::vector<String> keys);
    void restoreFileOperations(const RestoreInformation & restore_information);

    /// Remove 'path' prefix from 'key' to get relative key.
    /// It's needed to store keys to metadata files in RELATIVE_PATHS version.
    static String shrinkKey(const String & path, const String & key);
    std::tuple<UInt64, String> extractRevisionAndOperationFromKey(const String & key);

    /// Forms detached path '../../detached/part_name/' from '../../part_name/'
    static String pathToDetached(const String & source_path);

    const String bucket;

    MultiVersion<DiskS3Settings> current_settings;
    /// Gets disk settings from context.
    GetDiskSettings settings_getter;

    std::atomic<UInt64> revision_counter = 0;
    static constexpr UInt64 LATEST_REVISION = std::numeric_limits<UInt64>::max();
    static constexpr UInt64 UNKNOWN_REVISION = 0;

    /// File at path {metadata_path}/restore contains metadata restore information
    inline static const String RESTORE_FILE_NAME = "restore";

    /// Key has format: ../../r{revision}-{operation}
    const re2::RE2 key_regexp {".*/r(\\d+)-(\\w+).*"};

    /// Object contains information about schema version.
    inline static const String SCHEMA_VERSION_OBJECT = ".SCHEMA_VERSION";
    /// Version with possibility to backup-restore metadata.
    static constexpr int RESTORABLE_SCHEMA_VERSION = 1;
    /// Directories with data.
    const std::vector<String> data_roots {"data", "store"};
};

}

#endif
