#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <atomic>
#include <optional>
#include <base/logger_useful.h>
#include <base/FnTraits.h>
#include "Disks/DiskFactory.h"
#include "Disks/Executor.h"
#include <Disks/S3/S3Capabilities.h>

#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/ObjectIdentifier.h>

#include <Poco/DirectoryIterator.h>
#include <re2/re2.h>
#include <Disks/IDiskRemote.h>
#include <Common/FileCache_fwd.h>


namespace DB
{

/// Settings for DiskS3 that can be changed in runtime.
struct DiskS3Settings
{
    DiskS3Settings(
        const std::shared_ptr<Aws::S3::S3Client> & client_,
        size_t s3_max_single_read_retries_,
        size_t s3_min_upload_part_size_,
        size_t s3_upload_part_size_multiply_factor_,
        size_t s3_upload_part_size_multiply_parts_count_threshold_,
        size_t s3_max_single_part_upload_size_,
        size_t min_bytes_for_seek_,
        bool send_metadata_,
        int thread_pool_size_,
        int list_object_keys_size_,
        int objects_chunk_size_to_delete_);

    std::shared_ptr<Aws::S3::S3Client> client;
    size_t s3_max_single_read_retries;
    size_t s3_min_upload_part_size;
    size_t s3_upload_part_size_multiply_factor;
    size_t s3_upload_part_size_multiply_parts_count_threshold;
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
        DiskPtr metadata_disk_,
        FileCachePtr cache_,
        ContextPtr context_,
        const S3Capabilities & s3_capabilities_,
        SettingsPtr settings_,
        GetDiskSettings settings_getter_);

    std::unique_ptr<ReadBufferFromFileBase> readFile(
        const String & path,
        const ReadSettings & settings,
        std::optional<size_t> read_hint,
        std::optional<size_t> file_size) const override;

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

    DiskType getType() const override { return DiskType::S3; }
    bool isRemote() const override { return true; }

    bool supportZeroCopyReplication() const override { return true; }

    void shutdown() override;

    void startup() override;

    /// Check file exists and ClickHouse has an access to it
    /// Overrode in remote disk
    /// Required for remote disk to ensure that replica has access to data written by other node
    bool checkUniqueId(const String & id) const override;

    /// Dumps current revision counter into file 'revision.txt' at given path.
    void onFreeze(const String & path) override;

    void applyNewSettings(const Poco::Util::AbstractConfiguration & config, ContextPtr context, const String &, const DisksMap &) override;

    void setCapabilitiesSupportBatchDelete(bool value) { s3_capabilities.support_batch_delete = value; }

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
    void copyObject(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt) const;

    void copyObjectImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<std::reference_wrapper<const ObjectMetadata>> metadata = std::nullopt) const;
    void copyObjectMultipartImpl(const String & src_bucket, const String & src_key, const String & dst_bucket, const String & dst_key,
        std::optional<Aws::S3::Model::HeadObjectResult> head = std::nullopt,
        std::optional<std::reference_wrapper<const ObjectMetadata>> metadata = std::nullopt) const;

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
    S3Capabilities s3_capabilities;

    std::atomic<UInt64> revision_counter = 0;
    static constexpr UInt64 LATEST_REVISION = std::numeric_limits<UInt64>::max();
    static constexpr UInt64 UNKNOWN_REVISION = 0;

    /// File at path {metadata_path}/restore contains metadata restore information
    inline static const String RESTORE_FILE_NAME = "restore";

    /// Key has format: ../../r{revision}-{operation}
    const re2::RE2 key_regexp {".*/r(\\d+)-(\\w+)$"};

    /// Object contains information about schema version.
    inline static const String SCHEMA_VERSION_OBJECT = ".SCHEMA_VERSION";
    /// Version with possibility to backup-restore metadata.
    static constexpr int RESTORABLE_SCHEMA_VERSION = 1;
    /// Directories with data.
    const std::vector<String> data_roots {"data", "store"};

    ContextPtr context;
};

/// Helper class to collect keys into chunks of maximum size (to prepare batch requests to AWS API)
/// see https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html
class S3PathKeeper : public RemoteFSPathKeeper
{
public:
    using Chunk = Aws::Vector<Aws::S3::Model::ObjectIdentifier>;
    using Chunks = std::list<Chunk>;

    explicit S3PathKeeper(size_t chunk_limit_) : RemoteFSPathKeeper(chunk_limit_) {}

    void addPath(const String & path) override
    {
        if (chunks.empty() || chunks.back().size() >= chunk_limit)
        {
            /// add one more chunk
            chunks.push_back(Chunks::value_type());
            chunks.back().reserve(chunk_limit);
        }
        Aws::S3::Model::ObjectIdentifier obj;
        obj.SetKey(path);
        chunks.back().push_back(obj);
    }

    void removePaths(Fn<void(Chunk &&)> auto && remove_chunk_func)
    {
        for (auto & chunk : chunks)
            remove_chunk_func(std::move(chunk));
    }

    Chunks getChunks() const
    {
        return chunks;
    }

    static String getChunkKeys(const Chunk & chunk)
    {
        String res;
        for (const auto & obj : chunk)
        {
            const auto & key = obj.GetKey();
            if (!res.empty())
                res.append(", ");
            res.append(key.c_str(), key.size());
        }
        return res;
    }

private:
    Chunks chunks;
};

}

#endif
