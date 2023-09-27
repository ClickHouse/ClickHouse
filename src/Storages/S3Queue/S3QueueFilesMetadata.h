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

class S3QueueFilesMetadata
{
public:
    S3QueueFilesMetadata(const fs::path & zookeeper_path_, const S3QueueSettings & settings_);

    ~S3QueueFilesMetadata();

    bool trySetFileAsProcessing(const std::string & path);

    void setFileProcessed(const std::string & path);

    void setFileFailed(const std::string & path, const std::string & exception_message);

    using OnProgress = std::function<void(size_t)>;

    void deactivateCleanupTask();

    struct FileStatus
    {
        size_t processed_rows = 0;
        enum class State
        {
            Processing,
            Processed,
            Failed,
            None
        };
        State state = State::None;
        ProfileEvents::Counters profile_counters;

        time_t processing_start_time = 0;
        time_t processing_end_time = 0;
    };
    using FileStatuses = std::unordered_map<std::string, std::shared_ptr<FileStatus>>;

    std::shared_ptr<FileStatus> getFileStatus(const std::string & path);

    FileStatuses getFileStateses() const { return local_file_statuses.getAll(); }

    bool checkSettings(const S3QueueSettings & settings) const;

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

    Poco::Logger * log;

    std::atomic_bool shutdown = false;
    BackgroundSchedulePool::TaskHolder task;

    std::string getNodeName(const std::string & path);

    zkutil::ZooKeeperPtr getZooKeeper() const;

    void setFileProcessedForOrderedMode(const std::string & path);
    void setFileProcessedForUnorderedMode(const std::string & path);

    enum class SetFileProcessingResult
    {
        Success,
        ProcessingByOtherNode,
        AlreadyProcessed,
        AlreadyFailed,
    };
    SetFileProcessingResult trySetFileAsProcessingForOrderedMode(const std::string & path);
    SetFileProcessingResult trySetFileAsProcessingForUnorderedMode(const std::string & path);

    struct NodeMetadata
    {
        std::string file_path;
        UInt64 last_processed_timestamp = 0;
        std::string last_exception;
        UInt64 retries = 0;

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
        std::shared_ptr<FileStatus> get(const std::string & filename, bool create);
        bool remove(const std::string & filename, bool if_exists);
        FileStatus::State state(const std::string & filename) const;
        std::unique_lock<std::mutex> lock() const;
    };
    LocalFileStatuses local_file_statuses;
};

}
