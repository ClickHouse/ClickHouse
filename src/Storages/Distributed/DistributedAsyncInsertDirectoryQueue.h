#pragma once

#include <mutex>
#include <Client/ConnectionPool.h>
#include <Core/BackgroundSchedulePool.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Cluster.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Common/SettingsChanges.h>


namespace CurrentMetrics { class Increment; }

namespace DB
{

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class StorageDistributed;
class ActionBlocker;
class BackgroundSchedulePool;
class SettingsChanges;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;

class ISource;

/** Queue for async INSERT Into Distributed engine (distributed_foreground_insert=0).
 *
 * Files are added from two places:
 * - from filesystem at startup (StorageDistributed::startup())
 * - on INSERT via DistributedSink
 *
 * Later, in background, those files will be send to the remote nodes.
 *
 * The behaviour of this queue can be configured via the following settings:
 * - distributed_background_insert_batch
 * - distributed_background_insert_split_batch_on_failure
 * - distributed_background_insert_sleep_time_ms
 * - distributed_background_insert_max_sleep_time_ms
 */
class DistributedAsyncInsertDirectoryQueue
{
    friend class DistributedAsyncInsertBatch;

public:
    DistributedAsyncInsertDirectoryQueue(
        StorageDistributed & storage_,
        const DiskPtr & disk_,
        const std::string & relative_path_,
        ConnectionPoolWithFailoverPtr pool_,
        ActionBlocker & monitor_blocker_,
        BackgroundSchedulePool & bg_pool);

    ~DistributedAsyncInsertDirectoryQueue();

    static ConnectionPoolWithFailoverPtr createPool(const Cluster::Addresses & addresses, const StorageDistributed & storage);

    void updatePath(const std::string & new_relative_path);

    void flushAllData(const SettingsChanges & settings_changes);

    void shutdownAndDropAllData();

    void shutdownWithoutFlush();

    static std::shared_ptr<ISource> createSourceFromFile(const String & file_name);

    /// For scheduling via DistributedSink.
    bool addFileAndSchedule(const std::string & file_path, size_t file_size, size_t ms);

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

    void addFile(const std::string & file_path);
    void initializeFilesFromDisk();
    void processFiles(const SettingsChanges & settings_changes = {});
    void processFile(std::string & file_path, const SettingsChanges & settings_changes);
    void processFilesWithBatching(const SettingsChanges & settings_changes);

    void markAsBroken(const std::string & file_path);
    void markAsSend(const std::string & file_path);

    SyncGuardPtr getDirectorySyncGuard(const std::string & path);

    std::string getLoggerName() const;

    StorageDistributed & storage;
    const ConnectionPoolWithFailoverPtr pool;

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
    LoggerPtr log;
    ActionBlocker & monitor_blocker;

    BackgroundSchedulePoolTaskHolder task_handle;

    CurrentMetrics::Increment metric_pending_bytes;
    CurrentMetrics::Increment metric_pending_files;
    CurrentMetrics::Increment metric_broken_bytes;
    CurrentMetrics::Increment metric_broken_files;
};

}
