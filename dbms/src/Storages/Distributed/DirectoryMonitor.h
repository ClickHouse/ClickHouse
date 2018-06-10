#pragma once

#include <Storages/StorageDistributed.h>

#include <atomic>
#include <thread>
#include <mutex>
#include <condition_variable>


namespace DB
{

/** Details of StorageDistributed.
  * This type is not designed for standalone use.
  */
class StorageDistributedDirectoryMonitor
{
public:
    StorageDistributedDirectoryMonitor(StorageDistributed & storage, const std::string & name, const ConnectionPoolPtr & pool);
    ~StorageDistributedDirectoryMonitor();

    static ConnectionPoolPtr createPool(const std::string & name, const StorageDistributed & storage);

    void shutdownAndDropAllData();
private:
    void run();
    bool findFiles();
    void processFile(const std::string & file_path);
    void processFilesWithBatching(const std::map<UInt64, std::string> & files);

    static bool isFileBrokenErrorCode(int code);
    void markAsBroken(const std::string & file_path) const;
    bool maybeMarkAsBroken(const std::string & file_path, const Exception & e) const;

    std::string getLoggerName() const;

    StorageDistributed & storage;
    ConnectionPoolPtr pool;
    std::string path;

    bool should_batch_inserts = false;
    size_t min_batched_block_size_rows = 0;
    size_t min_batched_block_size_bytes = 0;
    String current_batch_file_path;

    struct BatchHeader;
    struct Batch;

    size_t error_count{};
    std::chrono::milliseconds default_sleep_time;
    std::chrono::milliseconds sleep_time;
    std::chrono::time_point<std::chrono::system_clock> last_decrease_time {std::chrono::system_clock::now()};
    std::atomic<bool> quit {false};
    std::mutex mutex;
    std::condition_variable cond;
    Logger * log;
    std::thread thread {&StorageDistributedDirectoryMonitor::run, this};
};

}
