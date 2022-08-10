#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Progress.h>
#include <Common/MemoryTracker.h>
#include <Common/OpenTelemetryTraceContext.h>
#include <Common/ProfileEvents.h>
#include <base/StringRef.h>
#include <Common/ConcurrentBoundedQueue.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>


namespace Poco
{
    class Logger;
}


namespace DB
{

class QueryStatus;
class ThreadStatus;
class QueryProfilerReal;
class QueryProfilerCPU;
class QueryThreadLog;
struct OpenTelemetrySpanHolder;
class TasksStatsCounters;
struct RUsageCounters;
struct PerfEventsCounters;
class TaskStatsInfoGetter;
class InternalTextLogsQueue;
struct ViewRuntimeData;
class QueryViewsLog;
class MemoryTrackerThreadSwitcher;
using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;

using InternalProfileEventsQueue = ConcurrentBoundedQueue<Block>;
using InternalProfileEventsQueuePtr = std::shared_ptr<InternalProfileEventsQueue>;
using InternalProfileEventsQueueWeakPtr = std::weak_ptr<InternalProfileEventsQueue>;
using ThreadStatusPtr = ThreadStatus *;

/** Thread group is a collection of threads dedicated to single task
  * (query or other process like background merge).
  *
  * ProfileEvents (counters) from a thread are propagated to thread group.
  *
  * Create via CurrentThread::initializeQuery (for queries) or directly (for various background tasks).
  * Use via CurrentThread::getGroup.
  */
class ThreadGroupStatus
{
public:
    struct ProfileEventsCountersAndMemory
    {
        ProfileEvents::Counters::Snapshot counters;
        Int64 memory_usage;
        UInt64 thread_id;
    };

    mutable std::mutex mutex;

    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker memory_tracker{VariableContext::Process};

    ContextWeakPtr query_context;
    ContextWeakPtr global_context;

    InternalTextLogsQueueWeakPtr logs_queue_ptr;
    InternalProfileEventsQueueWeakPtr profile_queue_ptr;
    std::function<void()> fatal_error_callback;

    std::vector<UInt64> thread_ids;
    std::unordered_set<ThreadStatusPtr> threads;

    /// The first thread created this thread group
    UInt64 master_thread_id = 0;

    LogsLevel client_logs_level = LogsLevel::none;

    String query;
    /// Query without new lines (see toOneLineQuery())
    /// Used to print in case of fatal error
    /// (to avoid calling extra code in the fatal error handler)
    String one_line_query;
    UInt64 normalized_query_hash = 0;

    std::vector<ProfileEventsCountersAndMemory> finished_threads_counters_memory;

    std::vector<ProfileEventsCountersAndMemory> getProfileEventsCountersAndMemoryForThreads();
};

using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wc++20-compat"
#endif
extern thread_local constinit ThreadStatus * current_thread;
#ifdef __clang__
#pragma clang diagnostic pop
#endif

/** Encapsulates all per-thread info (ProfileEvents, MemoryTracker, query_id, query context, etc.).
  * The object must be created in thread function and destroyed in the same thread before the exit.
  * It is accessed through thread-local pointer.
  *
  * This object should be used only via "CurrentThread", see CurrentThread.h
  */
class ThreadStatus : public boost::noncopyable
{
public:
    /// Linux's PID (or TGID) (the same id is shown by ps util)
    const UInt64 thread_id = 0;
    /// Also called "nice" value. If it was changed to non-zero (when attaching query) - will be reset to zero when query is detached.
    Int32 os_thread_priority = 0;

    /// TODO: merge them into common entity
    ProfileEvents::Counters performance_counters{VariableContext::Thread};
    MemoryTracker memory_tracker{VariableContext::Thread};

    /// Small amount of untracked memory (per thread atomic-less counter)
    Int64 untracked_memory = 0;
    /// Each thread could new/delete memory in range of (-untracked_memory_limit, untracked_memory_limit) without access to common counters.
    Int64 untracked_memory_limit = 4 * 1024 * 1024;

    /// Statistics of read and write rows/bytes
    Progress progress_in;
    Progress progress_out;

    using Deleter = std::function<void()>;
    Deleter deleter;

    // This is the current most-derived OpenTelemetry span for this thread. It
    // can be changed throughout the query execution, whenever we enter a new
    // span or exit it. See OpenTelemetrySpanHolder that is normally responsible
    // for these changes.
    OpenTelemetryTraceContext thread_trace_context;

protected:
    ThreadGroupStatusPtr thread_group;

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    /// Is set once
    ContextWeakPtr global_context;
    /// Use it only from current thread
    ContextWeakPtr query_context;

    String query_id;

    /// A logs queue used by TCPHandler to pass logs to a client
    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    InternalProfileEventsQueueWeakPtr profile_queue_ptr;

    bool performance_counters_finalized = false;
    UInt64 query_start_time_nanoseconds = 0;
    UInt64 query_start_time_microseconds = 0;
    time_t query_start_time = 0;
    size_t queries_started = 0;

    // CPU and Real time query profilers
    std::unique_ptr<QueryProfilerReal> query_profiler_real;
    std::unique_ptr<QueryProfilerCPU> query_profiler_cpu;

    Poco::Logger * log = nullptr;

    friend class CurrentThread;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> taskstats;

    /// Is used to send logs from logs_queue to client in case of fatal errors.
    std::function<void()> fatal_error_callback;

    /// It is used to avoid enabling the query profiler when you have multiple ThreadStatus in the same thread
    bool query_profiler_enabled = true;

    /// Requires access to query_id.
    friend class MemoryTrackerThreadSwitcher;
    void setQueryId(const String & query_id_)
    {
        query_id = query_id_;
    }

public:
    ThreadStatus();
    ~ThreadStatus();

    ThreadGroupStatusPtr getThreadGroup() const
    {
        return thread_group;
    }

    enum ThreadState
    {
        DetachedFromQuery = 0,  /// We just created thread or it is a background thread
        AttachedToQuery,        /// Thread executes enqueued query
        Died,                   /// Thread does not exist
    };

    int getCurrentState() const
    {
        return thread_state.load(std::memory_order_relaxed);
    }

    std::string_view getQueryId() const
    {
        return query_id;
    }

    auto getQueryContext() const
    {
        return query_context.lock();
    }

    auto getGlobalContext() const
    {
        return global_context.lock();
    }

    void disableProfiling()
    {
        assert(!query_profiler_real && !query_profiler_cpu);
        query_profiler_enabled = false;
    }

    /// Starts new query and create new thread group for it, current thread becomes master thread of the query
    void initializeQuery();

    /// Attaches slave thread to existing thread group
    void attachQuery(const ThreadGroupStatusPtr & thread_group_, bool check_detached = true);

    InternalTextLogsQueuePtr getInternalTextLogsQueue() const
    {
        return thread_state == Died ? nullptr : logs_queue_ptr.lock();
    }

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                     LogsLevel client_logs_level);

    InternalProfileEventsQueuePtr getInternalProfileEventsQueue() const
    {
        return thread_state == Died ? nullptr : profile_queue_ptr.lock();
    }

    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);

    /// Callback that is used to trigger sending fatal error messages to client.
    void setFatalErrorCallback(std::function<void()> callback);
    void onFatalError();

    /// Sets query context for current master thread and its thread group
    /// NOTE: query_context have to be alive until detachQuery() is called
    void attachQueryContext(ContextPtr query_context);

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Set the counters last usage to now
    void resetPerformanceCountersLastUsage();

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachQuery(bool exit_if_already_detached = false, bool thread_exits = false);

    void logToQueryViewsLog(const ViewRuntimeData & vinfo);

protected:
    void applyQuerySettings();

    void initPerformanceCounters();

    void initQueryProfiler();

    void finalizeQueryProfiler();

    void logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database, std::chrono::time_point<std::chrono::system_clock> now);


    void assertState(const std::initializer_list<int> & permitted_states, const char * description = nullptr) const;


private:
    void setupState(const ThreadGroupStatusPtr & thread_group_);
};

/**
 * Creates ThreadStatus for the main thread.
 */
class MainThreadStatus : public ThreadStatus
{
public:
    static MainThreadStatus & getInstance();
    static ThreadStatus * get() { return main_thread; }
    static bool isMainThread() { return main_thread == current_thread; }

    ~MainThreadStatus();

private:
    MainThreadStatus();

    static ThreadStatus * main_thread;
};

}
