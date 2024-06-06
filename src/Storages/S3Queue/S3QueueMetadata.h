#pragma once
#include "config.h"

#include <filesystem>
#include <Core/Types.h>
#include <Core/SettingsEnums.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/S3Queue/S3QueueIFileMetadata.h>
#include <Storages/S3Queue/S3QueueOrderedFileMetadata.h>
#include <Storages/S3Queue/S3QueueSettings.h>

namespace fs = std::filesystem;
namespace Poco { class Logger; }

namespace DB
{
struct S3QueueSettings;
class StorageS3Queue;
struct S3QueueTableMetadata;
struct StorageInMemoryMetadata;
using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

/**
 * A class for managing S3Queue metadata in zookeeper, e.g.
 * the following folders:
 * - <path_to_metadata>/processed
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * In case we use buckets for processing for Ordered mode, the structure looks like:
 * - <path_to_metadata>/buckets/<bucket>/processed -- persistent node, information about last processed file.
 * - <path_to_metadata>/buckets/<bucket>/lock -- ephemeral node, used for acquiring bucket lock.
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/failed
 *
 * Depending on S3Queue processing mode (ordered or unordered)
 * we can differently store metadata in /processed node.
 *
 * Implements caching of zookeeper metadata for faster responses.
 * Cached part is located in LocalFileStatuses.
 *
 * In case of Unordered mode - if files TTL is enabled or maximum tracked files limit is set
 * starts a background cleanup thread which is responsible for maintaining them.
 */
class S3QueueMetadata
{
public:
    using FileStatus = S3QueueIFileMetadata::FileStatus;
    using FileMetadataPtr = std::shared_ptr<S3QueueIFileMetadata>;
    using FileStatusPtr = std::shared_ptr<FileStatus>;
    using FileStatuses = std::unordered_map<std::string, FileStatusPtr>;
    using Bucket = size_t;
    using Processor = std::string;

    S3QueueMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_);
    ~S3QueueMetadata();

    void initialize(const ConfigurationPtr & configuration, const StorageInMemoryMetadata & storage_metadata);
    void checkSettings(const S3QueueSettings & settings) const;
    void shutdown();

    FileMetadataPtr getFileMetadata(const std::string & path, S3QueueOrderedFileMetadata::BucketInfoPtr bucket_info = {});

    FileStatusPtr getFileStatus(const std::string & path);
    FileStatuses getFileStatuses() const;

    /// Method of Ordered mode parallel processing.
    bool useBucketsForProcessing() const;
    Bucket getBucketForPath(const std::string & path) const;
    S3QueueOrderedFileMetadata::BucketHolderPtr tryAcquireBucket(const Bucket & bucket, const Processor & processor);

    static size_t getBucketsNum(const S3QueueSettings & settings);
    static size_t getBucketsNum(const S3QueueTableMetadata & settings);

private:
    void cleanupThreadFunc();
    void cleanupThreadFuncImpl();

    const S3QueueSettings settings;
    const fs::path zookeeper_path;
    const size_t buckets_num;

    LoggerPtr log;

    std::atomic_bool shutdown_called = false;
    BackgroundSchedulePool::TaskHolder task;

    class LocalFileStatuses;
    std::shared_ptr<LocalFileStatuses> local_file_statuses;
};

}
