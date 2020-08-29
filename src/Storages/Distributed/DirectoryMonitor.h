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
        StorageDistributed & storage_, std::string path_, ConnectionPoolPtr pool_, ActionBlocker & monitor_blocker_, BackgroundSchedulePool & bg_pool_);

    ~StorageDistributedDirectoryMonitor();

    static ConnectionPoolPtr createPool(const std::string & name, const StorageDistributed & storage);

    void updatePath(const std::string & new_path);

    void flushAllData();

    void shutdownAndDropAllData();

    static BlockInputStreamPtr createStreamFromFile(const String & file_name);

    /// For scheduling via DistributedBlockOutputStream
    bool scheduleAfter(size_t ms);

    /// system.distribution_queue interface
    struct Status
    {
        std::string path;
        std::exception_ptr last_exception;
        size_t error_count;
        size_t files_count;
        size_t bytes_count;
        bool is_blocked;
    };
    Status getStatus() const;

private:
    void run();

    std::map<UInt64, std::string> getFiles(CurrentMetrics::Increment & metric_pending_files);
    bool processFiles(const std::map<UInt64, std::string> & files, CurrentMetrics::Increment & metric_pending_files);
    void processFile(const std::string & file_path, CurrentMetrics::Increment & metric_pending_files);
    void processFilesWithBatching(const std::map<UInt64, std::string> & files, CurrentMetrics::Increment & metric_pending_files);

    static bool isFileBrokenErrorCode(int code);
    void markAsBroken(const std::string & file_path) const;
    bool maybeMarkAsBroken(const std::string & file_path, const Exception & e) const;

    std::string getLoggerName() const;

    StorageDistributed & storage;
    const ConnectionPoolPtr pool;
    std::string path;

    const bool should_batch_inserts = false;
    const size_t min_batched_block_size_rows = 0;
    const size_t min_batched_block_size_bytes = 0;
    String current_batch_file_path;

    struct BatchHeader;
    struct Batch;

    mutable std::mutex metrics_mutex;
    size_t error_count = 0;
    size_t files_count = 0;
    size_t bytes_count = 0;
    std::exception_ptr last_exception;

    const std::chrono::milliseconds default_sleep_time;
    std::chrono::milliseconds sleep_time;
    const std::chrono::milliseconds max_sleep_time;
    std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
    std::atomic<bool> quit {false};
    std::mutex mutex;
    Poco::Logger * log;
    ActionBlocker & monitor_blocker;

    BackgroundSchedulePool & bg_pool;
    BackgroundSchedulePoolTaskHolder task_handle;

    /// Read insert query and insert settings for backward compatible.
    static void readHeader(ReadBuffer & in, Settings & insert_settings, std::string & insert_query, ClientInfo & client_info, Poco::Logger * log);

    friend class DirectoryMonitorBlockInputStream;
};

}
