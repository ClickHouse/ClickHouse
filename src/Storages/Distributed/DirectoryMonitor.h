#pragma once

#include <Core/BackgroundSchedulePool.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Client/ConnectionPool.h>

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <IO/ReadBufferFromFile.h>


namespace CurrentMetrics { class Increment; }

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class StorageDistributed;
class ActionBlocker;
class BackgroundSchedulePool;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class ISource;

/** Details of StorageDistributed.
  * This type is not designed for standalone use.
  */
class StorageDistributedDirectoryMonitor
{
public:
    StorageDistributedDirectoryMonitor(
        StorageDistributed & storage_,
        const DiskPtr & disk_,
        const std::string & relative_path_,
        ConnectionPoolPtr pool_,
        ActionBlocker & monitor_blocker_,
        BackgroundSchedulePool & bg_pool,
        bool initialize_from_disk);

    ~StorageDistributedDirectoryMonitor();

    static ConnectionPoolPtr createPool(const std::string & name, const StorageDistributed & storage);

    void updatePath(const std::string & new_relative_path);

    void flushAllData();

    void shutdownAndDropAllData();

    static std::shared_ptr<ISource> createSourceFromFile(const String & file_name);

    /// For scheduling via DistributedSink.
    bool addAndSchedule(const std::string & file_path, size_t file_size, size_t ms);

    struct InternalStatus
    {
        std::exception_ptr last_exception;
        std::chrono::system_clock::time_point last_exception_time;

        size_t error_count = 0;

        size_t files_count = 0;
        size_t bytes_count = 0;

        size_t broken_files_count = 0;
        size_t broken_bytes_count = 0;
    };
    /// system.distribution_queue interface
    struct Status : InternalStatus
    {
        std::string path;
        bool is_blocked = false;
    };
    Status getStatus();

private:
    void run();

    bool hasPendingFiles() const;

    void initializeFilesFromDisk();
    void processFiles();
    void processFile(const std::string & file_path);
    void processFilesWithBatching();

    void markAsBroken(const std::string & file_path);
    void markAsSend(const std::string & file_path);

    std::string getLoggerName() const;

    StorageDistributed & storage;
    const ConnectionPoolPtr pool;

    DiskPtr disk;
    std::string relative_path;
    std::string path;
    std::string broken_relative_path;
    std::string broken_path;

    const bool should_batch_inserts = false;
    const bool split_batch_on_failure = true;
    const bool dir_fsync = false;
    const size_t min_batched_block_size_rows = 0;
    const size_t min_batched_block_size_bytes = 0;

    /// This is pending data (due to some error) for should_batch_inserts==true
    std::string current_batch_file_path;
    /// This is pending data (due to some error) for should_batch_inserts==false
    std::string current_file;

    struct BatchHeader;
    struct Batch;

    std::mutex status_mutex;

    InternalStatus status;

    ConcurrentBoundedQueue<std::string> pending_files;

    const std::chrono::milliseconds default_sleep_time;
    std::chrono::milliseconds sleep_time;
    const std::chrono::milliseconds max_sleep_time;
    std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
    std::mutex mutex;
    Poco::Logger * log;
    ActionBlocker & monitor_blocker;

    BackgroundSchedulePoolTaskHolder task_handle;

    CurrentMetrics::Increment metric_pending_files;
    CurrentMetrics::Increment metric_broken_files;
};

}
