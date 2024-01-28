#pragma once
#include "config.h"

#include <filesystem>
#include <Core/Types.h>
#include <Core/SettingsEnums.h>
#include <Core/BackgroundSchedulePool.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace fs = std::filesystem;
namespace Poco { class Logger; }

namespace DB
{
struct S3QueueSettings;
class StorageS3Queue;

/**
 * A class for managing S3Queue metadata in zookeeper, e.g.
 * the following folders:
 * - <path_to_metadata>/processing
 * - <path_to_metadata>/processed
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
    class ProcessingNodeHolder;
    using ProcessingNodeHolderPtr = std::shared_ptr<ProcessingNodeHolder>;

    S3QueueFilesMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_);

    ~S3QueueFilesMetadata();

    void setFileProcessed(ProcessingNodeHolderPtr holder);

    void setFileFailed(ProcessingNodeHolderPtr holder, const std::string & exception_message);

    struct FileStatus
    {
        enum class State
        {
            Processing,
            Processed,
            Failed,
            None
        };
        State state = State::None;

        std::atomic<size_t> processed_rows = 0;
        time_t processing_start_time = 0;
        time_t processing_end_time = 0;
        size_t retries = 0;
        std::string last_exception;
        ProfileEvents::Counters profile_counters;

        std::mutex processing_lock;
        std::mutex metadata_lock;
    };
    using FileStatusPtr = std::shared_ptr<FileStatus>;
    using FileStatuses = std::unordered_map<std::string, FileStatusPtr>;

    /// Set file as processing, if it is not alreaty processed, failed or processing.
    ProcessingNodeHolderPtr trySetFileAsProcessing(const std::string & path);

    FileStatusPtr getFileStatus(const std::string & path);

    FileStatuses getFileStateses() const { return local_file_statuses.getAll(); }

    bool checkSettings(const S3QueueSettings & settings) const;

    void deactivateCleanupTask();

private:
    const S3QueueMode mode;
    const UInt64 max_set_size;
    const UInt64 max_set_age_sec;
    const UInt64 max_loading_retries;
    const size_t min_cleanup_interval_ms;
    const size_t max_cleanup_interval_ms;

    const fs::path zookeeper_processing_path;
    const fs::path zookeeper_processed_path;
    const fs::path zookeeper_failed_path;
    const fs::path zookeeper_cleanup_lock_path;

    LoggerPtr log;

    std::atomic_bool shutdown = false;
    BackgroundSchedulePool::TaskHolder task;

    std::string getNodeName(const std::string & path);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    void setFileProcessedForOrderedMode(ProcessingNodeHolderPtr holder);
    void setFileProcessedForUnorderedMode(ProcessingNodeHolderPtr holder);

    enum class SetFileProcessingResult
    {
        Success,
        ProcessingByOtherNode,
        AlreadyProcessed,
        AlreadyFailed,
    };
    std::pair<SetFileProcessingResult, ProcessingNodeHolderPtr> trySetFileAsProcessingForOrderedMode(const std::string & path, const FileStatusPtr & file_status);
    std::pair<SetFileProcessingResult, ProcessingNodeHolderPtr> trySetFileAsProcessingForUnorderedMode(const std::string & path, const FileStatusPtr & file_status);

    struct NodeMetadata
    {
        std::string file_path;
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;
        std::string processing_id; /// For ephemeral processing node.

        std::string toString() const;
        static NodeMetadata fromString(const std::string & metadata_str);
    };

    NodeMetadata createNodeMetadata(const std::string & path, const std::string & exception = "", size_t retries = 0);

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

class S3QueueFilesMetadata::ProcessingNodeHolder
{
    friend class S3QueueFilesMetadata;
public:
    ProcessingNodeHolder(
        const std::string & processing_id_,
        const std::string & path_,
        const std::string & zk_node_path_,
        FileStatusPtr file_status_,
        zkutil::ZooKeeperPtr zk_client_);

    ~ProcessingNodeHolder();

    FileStatusPtr getFileStatus() { return file_status; }

private:
    bool remove(Coordination::Requests * requests = nullptr, Coordination::Responses * responses = nullptr);

    zkutil::ZooKeeperPtr zk_client;
    FileStatusPtr file_status;
    std::string path;
    std::string zk_node_path;
    std::string processing_id;
    bool removed = false;
    LoggerPtr log;
};

}
