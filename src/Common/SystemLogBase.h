#pragma once

#include <condition_variable>
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
    M(MetricLogElement) \
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
    M(ErrorLogElement)

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
    virtual String getName() const = 0;

    //// force -- force table creation (used for SYSTEM FLUSH LOGS)
    virtual void flush(bool force = false) = 0; /// NOLINT
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
    using Index = uint64_t;

public:
    explicit SystemLogQueue(const SystemLogQueueSettings & settings_);

    void shutdown();

    // producer methods
    void push(LogElement && element);
    Index notifyFlush(bool should_prepare_tables_anyway);
    void waitFlush(Index expected_flushed_up_to);

    /// Handles crash, flushes log without blocking if notify_flush_on_crash is set
    void handleCrash();

     // consumer methods
    Index pop(std::vector<LogElement>& output, bool & should_prepare_tables_anyway, bool & exit_this_thread);
    void confirm(Index to_flush_end);

private:
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
    // A flag that says we must create the tables even if the queue is empty.
    bool is_force_prepare_tables = false;
    // Requested to flush logs up to this index, exclusive
    Index requested_flush_up_to = 0;
    // Flushed log up to this index, exclusive
    Index flushed_up_to = 0;
    // Logged overflow message at this queue front index
    Index logged_queue_full_at_index = -1;

    bool is_shutdown = false;

    std::condition_variable flush_event;

    const SystemLogQueueSettings settings;
};


template <typename LogElement>
class SystemLogBase : public ISystemLog
{
public:
    using Self = SystemLogBase;

    explicit SystemLogBase(
        const SystemLogQueueSettings & settings_,
        std::shared_ptr<SystemLogQueue<LogElement>> queue_ = nullptr);

    void startup() override;

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(LogElement element);

    /// Flush data in the buffer to disk. Block the thread until the data is stored on disk.
    void flush(bool force) override;

    /// Handles crash, flushes log without blocking if notify_flush_on_crash is set
    void handleCrash() override;

    /// Non-blocking flush data in the buffer to disk.
    void notifyFlush(bool force);

    String getName() const override { return LogElement::name(); }

    static const char * getDefaultOrderBy() { return "event_date, event_time"; }
    static consteval size_t getDefaultMaxSize() { return 1048576; }
    static consteval size_t getDefaultReservedSize() { return 8192; }
    static consteval size_t getDefaultFlushIntervalMilliseconds() { return 7500; }
    static consteval bool shouldNotifyFlushOnCrash() { return false; }
    static consteval bool shouldTurnOffLogger() { return false; }

protected:
    std::shared_ptr<SystemLogQueue<LogElement>> queue;
};
}
