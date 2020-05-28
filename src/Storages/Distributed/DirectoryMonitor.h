#pragma once

#include <Storages/StorageDistributed.h>
#include <Core/BackgroundSchedulePool.h>

#include <atomic>
#include <mutex>
#include <condition_variable>
#include <IO/ReadBufferFromFile.h>


namespace CurrentMetrics { class Increment; }

namespace DB
{

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
private:
    void run();
    bool processFiles(CurrentMetrics::Increment & metric_pending_files);
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

    size_t error_count{};
    const std::chrono::milliseconds default_sleep_time;
    std::chrono::milliseconds sleep_time;
    const std::chrono::milliseconds max_sleep_time;
    std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
    std::atomic<bool> quit {false};
    std::mutex mutex;
    Logger * log;
    ActionBlocker & monitor_blocker;

    BackgroundSchedulePool & bg_pool;
    BackgroundSchedulePoolTaskHolder task_handle;

    /// Read insert query and insert settings for backward compatible.
    static void readHeader(ReadBuffer & in, Settings & insert_settings, std::string & insert_query, ClientInfo & client_info, Logger * log);

    friend class DirectoryMonitorBlockInputStream;
};

}
