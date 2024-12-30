#pragma once

#include <condition_variable>
#include <limits>
#include <memory>
#include <vector>
#include <base/types.h>

#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <Common/ThreadPool_fwd.h>

#define SYSTEM_LOG_ELEMENTS(M) \
    M(AsynchronousMetricLogElement) \
    M(CrashLogElement) \
    M(OpenTelemetrySpanLogElement) \
    M(PartLogElement) \
    M(QueryLogElement) \
    M(QueryThreadLogElement) \
    M(QueryViewsLogElement) \
    M(SessionLogElement) \
    M(TraceLogElement) \
    M(TransactionsInfoLogElement) \
    M(ZooKeeperLogElement) \
    M(ProcessorProfileLogElement) \
    M(TextLogElement) \
    M(ObjectStorageQueueLogElement) \
    M(FilesystemCacheLogElement) \
    M(FilesystemReadPrefetchesLogElement) \
    M(AsynchronousInsertLogElement) \
    M(BackupLogElement) \
    M(BlobStorageLogElement) \
    M(QueryMetricLogElement)

namespace Poco
{

class Logger;

namespace Util
{
    class AbstractConfiguration;
}

}


namespace DB
{

struct StorageID;

class ISystemLog
{
public:
    using Index = int64_t;

    virtual String getName() const = 0;

    /// Return the index of the latest added log element. That index no less than the flashed index.
    /// The flashed index is the index of the last log element which has been flushed successfully.
    /// Thereby all the records whose index is less than the flashed index are flushed already.
    virtual Index getLastLogIndex() = 0;
    /// Call this method to wake up the flush thread and flush the data in the background. It is non blocking call
    virtual void notifyFlush(Index expected_flushed_index, bool should_prepare_tables_anyway) = 0;
    /// Call this method to wait intill the logs are flushed up to expected_flushed_index. It is blocking call.
    virtual void flush(Index expected_flushed_index, bool should_prepare_tables_anyway) = 0;

    virtual void prepareTable() = 0;

    /// Start the background thread.
    virtual void startup() = 0;

    /// Stop the background flush thread before destructor. No more data will be written.
    virtual void shutdown() = 0;

    virtual void stopFlushThread() = 0;

    /// Handles crash, flushes log without blocking if notify_flush_on_crash is set
    virtual void handleCrash() = 0;

    virtual ~ISystemLog();

    virtual void savingThreadFunction() = 0;

protected:
    std::mutex thread_mutex;
    std::unique_ptr<ThreadFromGlobalPool> saving_thread;

    bool is_shutdown = false;
};

struct SystemLogQueueSettings
{
    String database;
    String table;
    size_t reserved_size_rows;
    size_t max_size_rows;
    size_t buffer_size_rows_flush_threshold;
    size_t flush_interval_milliseconds;
    bool notify_flush_on_crash;
    bool turn_off_logger;
};

template <typename LogElement>
class SystemLogQueue
{
public:
    using Index = ISystemLog::Index;

    explicit SystemLogQueue(const SystemLogQueueSettings & settings_);

    void shutdown();

    // producer methods
    void push(LogElement && element);

    Index getLastLogIndex();
    void notifyFlush(Index expected_flushed_index, bool should_prepare_tables_anyway);
    void waitFlush(Index expected_flushed_index, bool should_prepare_tables_anyway);

    /// Handles crash, flushes log without blocking if notify_flush_on_crash is set
    void handleCrash();

    struct PopResult
    {
        Index last_log_index = 0;
        std::vector<LogElement> logs = {};
        bool create_table_force = false;
        bool is_shutdown = false;
    };

     // consumer methods
    PopResult pop();
    void confirm(Index last_flashed_index);

private:
    void notifyFlushUnlocked(Index expected_flushed_index, bool should_prepare_tables_anyway);

    /// Data shared between callers of add()/flush()/shutdown(), and the saving thread
    std::mutex mutex;

    LoggerPtr log;

    // Queue is bounded. But its size is quite large to not block in all normal cases.
    std::vector<LogElement> queue;

    // An always-incrementing index of the first message currently in the queue.
    // We use it to give a global sequential index to every message, so that we
    // can wait until a particular message is flushed. This is used to implement
    // synchronous log flushing for SYSTEM FLUSH LOGS.
    Index queue_front_index = 0;

    // Requested to flush logs up to this index, exclusive
    Index requested_flush_index = std::numeric_limits<Index>::min();
    // Flushed log up to this index, exclusive
    Index flushed_index = 0;

    // The same logic for the prepare tables: if requested_prepar_tables > prepared_tables we need to do prepare
    // except that initial prepared_tables is -1
    // it is due to the difference: when no logs have been written and we call flush logs
    // it becomes in the state: requested_flush_index = 0 and flushed_index = 0 -- we do not want to do anything
    // but if we need to prepare tables it becomes requested_prepare_tables = 0 and prepared_tables = -1
    // we trigger background thread and do prepare
    Index requested_prepare_tables = std::numeric_limits<Index>::min();
    Index prepared_tables = -1;

    size_t ignored_logs = 0;

    bool is_shutdown = false;

    std::condition_variable confirm_event;
    std::condition_variable flush_event;

    const SystemLogQueueSettings settings;
};


template <typename LogElement>
class SystemLogBase : public ISystemLog
{
public:
    using Index = ISystemLog::Index;
    using Self = SystemLogBase;

    explicit SystemLogBase(
        const SystemLogQueueSettings & settings_,
        std::shared_ptr<SystemLogQueue<LogElement>> queue_ = nullptr);

    void startup() override;

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(LogElement element);

    Index getLastLogIndex() override;

    void notifyFlush(Index expected_flushed_index, bool should_prepare_tables_anyway) override;

    /// Flush data in the buffer to disk. Block the thread until the data is stored on disk.
    void flush(Index expected_flushed_index, bool should_prepare_tables_anyway) override;

    /// Handles crash, flushes log without blocking if notify_flush_on_crash is set
    void handleCrash() override;

    String getName() const override { return LogElement::name(); }

    static const char * getDefaultOrderBy() { return "event_date, event_time"; }
    static consteval size_t getDefaultMaxSize() { return 1048576; }
    static consteval size_t getDefaultReservedSize() { return 8192; }
    static consteval size_t getDefaultFlushIntervalMilliseconds() { return 7500; }
    static consteval bool shouldNotifyFlushOnCrash() { return false; }
    static consteval bool shouldTurnOffLogger() { return false; }

protected:
    void stopFlushThread() final;

    std::shared_ptr<SystemLogQueue<LogElement>> queue;
};
}
