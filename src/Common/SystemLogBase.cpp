#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/ErrorLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/ObjectStorageQueueLog.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <Interpreters/BackupLog.h>
#include <Interpreters/PeriodicLog.h>
#include <IO/S3/BlobStorageLogWriter.h>

#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SystemLogBase.h>
#include <Common/ThreadPool.h>

#include <Common/logger_useful.h>
#include <base/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
    extern const int ABORTED;
}

ISystemLog::~ISystemLog() = default;


template <typename LogElement>
SystemLogQueue<LogElement>::SystemLogQueue(const SystemLogQueueSettings & settings_)
    : log(getLogger("SystemLogQueue (" + settings_.database + "." +settings_.table + ")"))
    , settings(settings_)

{
    queue.reserve(settings.reserved_size_rows);

    if (settings.turn_off_logger)
        log->setLevel(0);
}

static thread_local bool recursive_push_call = false;

template <typename LogElement>
void SystemLogQueue<LogElement>::push(LogElement&& element)
{
    /// It is possible that the method will be called recursively.
    /// Better to drop these events to avoid complications.
    if (recursive_push_call)
        return;
    recursive_push_call = true;
    SCOPE_EXIT({ recursive_push_call = false; });

    /// Memory can be allocated while resizing on queue.push_back.
    /// The size of allocation can be in order of a few megabytes.
    /// But this should not be accounted for query memory usage.
    /// Otherwise the tests like 01017_uniqCombined_memory_usage.sql will be flaky.
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

    /// Should not log messages under mutex.
    bool buffer_size_rows_flush_threshold_exceeded = false;

    {
        std::unique_lock lock(mutex);

        if (is_shutdown)
            return;

        if (queue.size() == settings.buffer_size_rows_flush_threshold)
        {
            buffer_size_rows_flush_threshold_exceeded = true;

            // The queue more than half full, time to flush.
            // We only check for strict equality, because messages are added one
            // by one, under exclusive lock, so we will see each message count.
            // It is enough to only wake the flushing thread once, after the message
            // count increases past half available size.

            const auto last_log_index = queue_front_index + queue.size();
            notifyFlushUnlocked(last_log_index, /* should_prepare_tables_anyway */ false);
        }

        if (queue.size() >= settings.max_size_rows)
        {
            chassert(queue.size() == settings.max_size_rows);

            // Ignore all further entries until the queue is flushed.
            // To the next batch we add a log message about how much we have lost
            ++ignored_logs;
            return;
        }

        queue.push_back(std::move(element));
    }

    if (buffer_size_rows_flush_threshold_exceeded)
        LOG_INFO(log, "Queue is half full for system log '{}'. buffer_size_rows_flush_threshold {}",
                 demangle(typeid(*this).name()), settings.buffer_size_rows_flush_threshold);
}

template <typename LogElement>
void SystemLogQueue<LogElement>::handleCrash()
{
    if (settings.notify_flush_on_crash)
    {
        notifyFlush(getLastLogIndex(), /* should_prepare_tables_anyway */ true);
    }
}

template <typename LogElement>
void SystemLogQueue<LogElement>::notifyFlushUnlocked(Index expected_flushed_index, bool should_prepare_tables_anyway)
{
    if (should_prepare_tables_anyway)
        requested_prepare_tables = std::max(requested_prepare_tables, expected_flushed_index);

    requested_flush_index = std::max(requested_flush_index, expected_flushed_index);

    flush_event.notify_all();
}

template <typename LogElement>
void SystemLogQueue<LogElement>::notifyFlush(SystemLogQueue<LogElement>::Index expected_flushed_index, bool should_prepare_tables_anyway)
{
    std::lock_guard lock(mutex);
    notifyFlushUnlocked(expected_flushed_index, should_prepare_tables_anyway);
}

template <typename LogElement>
void SystemLogQueue<LogElement>::waitFlush(SystemLogQueue<LogElement>::Index expected_flushed_index, bool should_prepare_tables_anyway)
{
    LOG_DEBUG(log, "Requested flush up to offset {}", expected_flushed_index);

    // Use an arbitrary timeout to avoid endless waiting. 60s proved to be
    // too fast for our parallel functional tests, probably because they
    // heavily load the disk.
    const int timeout_seconds = 180;

    std::unique_lock lock(mutex);

    // there is no obligation to call notifyFlush before waitFlush, than we have to be sure that flush_event has been triggered before we wait the result
    notifyFlushUnlocked(expected_flushed_index, should_prepare_tables_anyway);

    auto result = confirm_event.wait_for(lock, std::chrono::seconds(timeout_seconds), [&]
    {
        if (should_prepare_tables_anyway)
            return (flushed_index >= expected_flushed_index && prepared_tables >= requested_prepare_tables) || is_shutdown;
        return (flushed_index >= expected_flushed_index) || is_shutdown;
    });

    if (!result)
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded ({} s) while flushing system log '{}'.",
            toString(timeout_seconds), demangle(typeid(*this).name()));
    }

    if (is_shutdown)
    {
        throw Exception(ErrorCodes::ABORTED, "Shutdown has been called while flushing system log '{}'. Aborting.",
            demangle(typeid(*this).name()));
    }
}

template <typename LogElement>
SystemLogQueue<LogElement>::Index SystemLogQueue<LogElement>::getLastLogIndex()
{
    std::lock_guard lock(mutex);
    return queue_front_index + queue.size();
}

template <typename LogElement>
void SystemLogQueue<LogElement>::confirm(SystemLogQueue<LogElement>::Index last_flashed_index)
{
    std::lock_guard lock(mutex);
    prepared_tables = std::max(prepared_tables, last_flashed_index);
    flushed_index = std::max(flushed_index, last_flashed_index);
    confirm_event.notify_all();
}

template <typename LogElement>
typename SystemLogQueue<LogElement>::PopResult SystemLogQueue<LogElement>::pop()
{
    PopResult result;
    size_t prev_ignored_logs = 0;

    {
        std::unique_lock lock(mutex);

        flush_event.wait_for(lock, std::chrono::milliseconds(settings.flush_interval_milliseconds), [&] ()
        {
            return requested_flush_index > flushed_index || requested_prepare_tables > prepared_tables || is_shutdown;
        });

        if (is_shutdown)
            return PopResult{.is_shutdown = true};

        queue_front_index += queue.size();
        prev_ignored_logs = ignored_logs;
        ignored_logs = 0;

        result.last_log_index = queue_front_index;
        result.logs.swap(queue);
        result.create_table_force = requested_prepare_tables > prepared_tables;
    }

    if (prev_ignored_logs)
        LOG_ERROR(log, "Queue had been full at {}, accepted {} logs, ignored {} logs.",
                    result.last_log_index - result.logs.size(),
                    result.logs.size(),
                    prev_ignored_logs);

    return result;
}

template <typename LogElement>
void SystemLogQueue<LogElement>::shutdown()
{
    std::unique_lock lock(mutex);
    is_shutdown = true;
    /// Tell thread to shutdown.
    flush_event.notify_all();
}

template <typename LogElement>
SystemLogBase<LogElement>::SystemLogBase(
    const SystemLogQueueSettings & settings_,
    std::shared_ptr<SystemLogQueue<LogElement>> queue_)
    : queue(queue_ ? queue_ : std::make_shared<SystemLogQueue<LogElement>>(settings_))
{
}

template <typename LogElement>
SystemLogBase<LogElement>::Index SystemLogBase<LogElement>::getLastLogIndex()
{
    return queue->getLastLogIndex();
}

template <typename LogElement>
void SystemLogBase<LogElement>::notifyFlush(Index expected_flushed_index, bool should_prepare_tables_anyway)
{
    queue->notifyFlush(expected_flushed_index, should_prepare_tables_anyway);
}

template <typename LogElement>
void SystemLogBase<LogElement>::flush(Index expected_flushed_index, bool should_prepare_tables_anyway)
{
    queue->waitFlush(expected_flushed_index, should_prepare_tables_anyway);
}

template <typename LogElement>
void SystemLogBase<LogElement>::handleCrash()
{
    queue->handleCrash();
}

template <typename LogElement>
void SystemLogBase<LogElement>::startup()
{
    std::lock_guard lock(thread_mutex);
    saving_thread = std::make_unique<ThreadFromGlobalPool>([this] { savingThreadFunction(); });
}

template <typename LogElement>
void SystemLogBase<LogElement>::stopFlushThread()
{
    {
        std::lock_guard lock(thread_mutex);

        if (!saving_thread || !saving_thread->joinable())
            return;

        if (is_shutdown)
            return;

        is_shutdown = true;
        queue->shutdown();
    }

    saving_thread->join();
}

template <typename LogElement>
void SystemLogBase<LogElement>::add(LogElement element)
{
    queue->push(std::move(element));
}

#define INSTANTIATE_SYSTEM_LOG_BASE(ELEMENT) template class SystemLogBase<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_BASE)
SYSTEM_PERIODIC_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_BASE)

#define INSTANTIATE_SYSTEM_LOG_QUEUE(ELEMENT) template class SystemLogQueue<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_QUEUE)
SYSTEM_PERIODIC_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_QUEUE)

}
