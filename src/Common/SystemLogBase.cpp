#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/ErrorLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
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
            const uint64_t queue_end = queue_front_index + queue.size();
            requested_flush_up_to = std::max(requested_flush_up_to, queue_end);

            flush_event.notify_all();
        }

        if (queue.size() >= settings.max_size_rows)
        {
            // Ignore all further entries until the queue is flushed.
            // Log a message about that. Don't spam it -- this might be especially
            // problematic in case of trace log. Remember what the front index of the
            // queue was when we last logged the message. If it changed, it means the
            // queue was flushed, and we can log again.
            if (queue_front_index != logged_queue_full_at_index)
            {
                logged_queue_full_at_index = queue_front_index;

                // TextLog sets its logger level to 0, so this log is a noop and
                // there is no recursive logging.
                lock.unlock();
                LOG_ERROR(log, "Queue is full for system log '{}' at {}. max_size_rows {}",
                          demangle(typeid(*this).name()),
                          queue_front_index,
                          settings.max_size_rows);
            }

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
        notifyFlush(/* force */ true);
}

template <typename LogElement>
void SystemLogQueue<LogElement>::waitFlush(uint64_t expected_flushed_up_to)
{
    // Use an arbitrary timeout to avoid endless waiting. 60s proved to be
    // too fast for our parallel functional tests, probably because they
    // heavily load the disk.
    const int timeout_seconds = 180;
    std::unique_lock lock(mutex);
    bool result = flush_event.wait_for(lock, std::chrono::seconds(timeout_seconds), [&]
    {
        return flushed_up_to >= expected_flushed_up_to && !is_force_prepare_tables;
    });

    if (!result)
    {
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded ({} s) while flushing system log '{}'.",
            toString(timeout_seconds), demangle(typeid(*this).name()));
    }
}

template <typename LogElement>
uint64_t SystemLogQueue<LogElement>::notifyFlush(bool should_prepare_tables_anyway)
{
    uint64_t this_thread_requested_offset;

    {
        std::lock_guard lock(mutex);
        if (is_shutdown)
            return uint64_t(-1);

        this_thread_requested_offset = queue_front_index + queue.size();

        // Publish our flush request, taking care not to overwrite the requests
        // made by other threads.
        is_force_prepare_tables |= should_prepare_tables_anyway;
        requested_flush_up_to = std::max(requested_flush_up_to, this_thread_requested_offset);

        flush_event.notify_all();
    }

    LOG_DEBUG(log, "Requested flush up to offset {}", this_thread_requested_offset);
    return this_thread_requested_offset;
}

template <typename LogElement>
void SystemLogQueue<LogElement>::confirm(uint64_t to_flush_end)
{
    std::lock_guard lock(mutex);
    flushed_up_to = to_flush_end;
    is_force_prepare_tables = false;
    flush_event.notify_all();
}

template <typename LogElement>
typename SystemLogQueue<LogElement>::Index SystemLogQueue<LogElement>::pop(std::vector<LogElement> & output,
                                                                           bool & should_prepare_tables_anyway,
                                                                           bool & exit_this_thread)
{
    /// Call dtors and deallocate strings without holding the global lock
    output.resize(0);

    std::unique_lock lock(mutex);
    flush_event.wait_for(lock,
        std::chrono::milliseconds(settings.flush_interval_milliseconds),
        [&] ()
        {
            return requested_flush_up_to > flushed_up_to || is_shutdown || is_force_prepare_tables;
        }
    );

    queue_front_index += queue.size();
    // Swap with existing array from previous flush, to save memory
    // allocations.
    queue.swap(output);

    should_prepare_tables_anyway = is_force_prepare_tables;

    exit_this_thread = is_shutdown;
    return queue_front_index;
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
void SystemLogBase<LogElement>::flush(bool force)
{
    uint64_t this_thread_requested_offset = queue->notifyFlush(force);
    if (this_thread_requested_offset == uint64_t(-1))
        return;

    queue->waitFlush(this_thread_requested_offset);
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
void SystemLogBase<LogElement>::add(LogElement element)
{
    queue->push(std::move(element));
}

template <typename LogElement>
void SystemLogBase<LogElement>::notifyFlush(bool force) { queue->notifyFlush(force); }

#define INSTANTIATE_SYSTEM_LOG_BASE(ELEMENT) template class SystemLogBase<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_BASE)

#define INSTANTIATE_SYSTEM_LOG_QUEUE(ELEMENT) template class SystemLogQueue<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG_QUEUE)

}
