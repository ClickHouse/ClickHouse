#pragma once
#include "config.h"

#include <filesystem>
#include <Core/Types.h>
#include <Core/SettingsEnums.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include "S3QueueIFileMetadata.h"

namespace fs = std::filesystem;
namespace Poco { class Logger; }

namespace DB
{
struct S3QueueSettings;
class StorageS3Queue;

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
class S3QueueFilesMetadata
{
public:
    using FileStatus = IFileMetadata::FileStatus;
    using FileMetadataPtr = std::shared_ptr<IFileMetadata>;
    using FileStatusPtr = std::shared_ptr<FileStatus>;
    using FileStatuses = std::unordered_map<std::string, FileStatusPtr>;
    using Bucket = size_t;
    using Processor = std::string;

    S3QueueFilesMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_);

    ~S3QueueFilesMetadata();

    FileMetadataPtr getFileMetadata(const std::string & path);

    FileStatusPtr getFileStatus(const std::string & path);

    FileStatuses getFileStateses() const { return local_file_statuses.getAll(); }

    bool checkSettings(const S3QueueSettings & settings) const;

    void shutdown();

    bool useBucketsForProcessing() const;
    /// Calculate which processing id corresponds to a given file path.
    /// The file will be processed by a thread related to this processing id.
    Bucket getBucketForPath(const std::string & path) const;

    bool tryAcquireBucket(const Bucket & bucket, const Processor & processor);
    void releaseBucket(const Bucket & bucket);

private:
    const S3QueueMode mode;
    const UInt64 max_set_size;
    const UInt64 max_set_age_sec;
    const UInt64 max_loading_retries;
    const size_t min_cleanup_interval_ms;
    const size_t max_cleanup_interval_ms;
    const size_t buckets_num;
    const fs::path zookeeper_path;

    LoggerPtr log;

    std::atomic_bool shutdown_called = false;
    BackgroundSchedulePool::TaskHolder task;

    fs::path getBucketLockPath(const Bucket & bucket) const;
    std::string getProcessorInfo(const std::string & processor_id);

    void cleanupThreadFunc();
    void cleanupThreadFuncImpl();

    struct LocalFileStatuses
    {
        FileStatuses file_statuses;
        mutable std::mutex mutex;

        FileStatuses getAll() const;
        FileStatusPtr get(const std::string & filename, bool create);
        bool remove(const std::string & filename, bool if_exists);
        std::unique_lock<std::mutex> lock() const;
    };
    LocalFileStatuses local_file_statuses;
};

}
