#pragma once

#include <Storages/StorageDistributed.h>
#include <Common/ThreadPool.h>

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

/** Details of StorageDistributed.
  * This type is not designed for standalone use.
  */
class StorageDistributedDirectoryMonitor
{
public:
    StorageDistributedDirectoryMonitor(
        StorageDistributed & storage_, std::string name_, ConnectionPoolPtr pool_, ActionBlocker & monitor_blocker_);

    ~StorageDistributedDirectoryMonitor();

    static ConnectionPoolPtr createPool(const std::string & name, const StorageDistributed & storage);

    void updatePath();

    void flushAllData();

    void shutdownAndDropAllData();
private:
    void run();
    bool processFiles();
    void processFile(const std::string & file_path);
    void processFilesWithBatching(const std::map<UInt64, std::string> & files);

    static bool isFileBrokenErrorCode(int code);
    void markAsBroken(const std::string & file_path) const;
    bool maybeMarkAsBroken(const std::string & file_path, const Exception & e) const;

    std::string getLoggerName() const;

    StorageDistributed & storage;
    const ConnectionPoolPtr pool;
    const std::string name;
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
    std::condition_variable cond;
    Logger * log;
    ActionBlocker & monitor_blocker;
    ThreadFromGlobalPool thread{&StorageDistributedDirectoryMonitor::run, this};

    /// Read insert query and insert settings for backward compatible.
    void readHeader(ReadBuffer & in, Settings & insert_settings, std::string & insert_query) const;
};

}
