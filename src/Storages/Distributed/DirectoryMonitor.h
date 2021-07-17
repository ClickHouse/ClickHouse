#pragma once

#include <Core/BackgroundSchedulePool.h>
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
        BackgroundSchedulePool & bg_pool);

    ~StorageDistributedDirectoryMonitor();

    static ConnectionPoolPtr createPool(const std::string & name, const StorageDistributed & storage);

    void updatePath(const std::string & new_relative_path);

    void flushAllData();

    void shutdownAndDropAllData();

    static BlockInputStreamPtr createStreamFromFile(const String & file_name);

    /// For scheduling via DistributedBlockOutputStream
    bool addAndSchedule(size_t file_size, size_t ms);

    struct InternalStatus
    {
        std::exception_ptr last_exception;

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

    std::map<UInt64, std::string> getFiles();
    bool processFiles(const std::map<UInt64, std::string> & files);
    void processFile(const std::string & file_path);
    void processFilesWithBatching(const std::map<UInt64, std::string> & files);

    void markAsBroken(const std::string & file_path);
    void markAsSend(const std::string & file_path);
    bool maybeMarkAsBroken(const std::string & file_path, const Exception & e);

    std::string getLoggerName() const;

    StorageDistributed & storage;
    const ConnectionPoolPtr pool;

    DiskPtr disk;
    std::string relative_path;
    std::string path;

    const bool should_batch_inserts = false;
    const bool split_batch_on_failure = true;
    const bool dir_fsync = false;
    const size_t min_batched_block_size_rows = 0;
    const size_t min_batched_block_size_bytes = 0;
    String current_batch_file_path;

    struct BatchHeader;
    struct Batch;

    std::mutex status_mutex;
    InternalStatus status;

    const std::chrono::milliseconds default_sleep_time;
    std::chrono::milliseconds sleep_time;
    const std::chrono::milliseconds max_sleep_time;
    std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
    std::atomic<bool> quit {false};
    std::mutex mutex;
    Poco::Logger * log;
    ActionBlocker & monitor_blocker;

    BackgroundSchedulePoolTaskHolder task_handle;

    CurrentMetrics::Increment metric_pending_files;
    CurrentMetrics::Increment metric_broken_files;

    friend class DirectoryMonitorBlockInputStream;
};

}
