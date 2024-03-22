#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Progress.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <base/StringRef.h>

#include <boost/noncopyable.hpp>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>


namespace Poco
{
    class Logger;
}


template <class T>
class ConcurrentBoundedQueue;

namespace DB
{

class QueryStatus;
class ThreadStatus;
class QueryProfilerReal;
class QueryProfilerCPU;
class QueryThreadLog;
class TasksStatsCounters;
struct RUsageCounters;
struct PerfEventsCounters;
class InternalTextLogsQueue;
struct ViewRuntimeData;
class QueryViewsLog;
using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;

using InternalProfileEventsQueue = ConcurrentBoundedQueue<Block>;
using InternalProfileEventsQueuePtr = std::shared_ptr<InternalProfileEventsQueue>;
using InternalProfileEventsQueueWeakPtr = std::weak_ptr<InternalProfileEventsQueue>;
using ThreadStatusPtr = ThreadStatus *;

using QueryIsCanceledPredicate = std::function<bool()>;

/** Thread group is a collection of threads dedicated to single task
  * (query or other process like background merge).
  *
  * ProfileEvents (counters) from a thread are propagated to thread group.
  *
  * Create via CurrentThread::initializeQuery (for queries) or directly (for various background tasks).
  * Use via CurrentThread::getGroup.
  */
class ThreadGroup;
using ThreadGroupPtr = std::shared_ptr<ThreadGroup>;

class ThreadGroup
{
public:
    ThreadGroup();
    using FatalErrorCallback = std::function<void()>;
    ThreadGroup(ContextPtr query_context_, FatalErrorCallback fatal_error_callback_ = {});

    /// The first thread created this thread group
    const UInt64 master_thread_id;

    /// Set up at creation, no race when reading
    const ContextWeakPtr query_context;
    const ContextWeakPtr global_context;

    const FatalErrorCallback fatal_error_callback;

    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker memory_tracker{VariableContext::Process};

    struct SharedData
    {
        InternalProfileEventsQueueWeakPtr profile_queue_ptr;

        InternalTextLogsQueueWeakPtr logs_queue_ptr;
        LogsLevel client_logs_level = LogsLevel::none;

        String query_for_logs;
        UInt64 normalized_query_hash = 0;

        QueryIsCanceledPredicate query_is_canceled_predicate = {};
    };

    SharedData getSharedData()
    {
        /// Critical section for making the copy of shared_data
        std::lock_guard lock(mutex);
        return shared_data;
    }

    /// Mutation shared data
    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue, LogsLevel logs_level);
    void attachQueryForLog(const String & query_, UInt64 normalized_hash = 0);
    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);

    /// When new query starts, new thread group is created for it, current thread becomes master thread of the query
    static ThreadGroupPtr createForQuery(ContextPtr query_context_, FatalErrorCallback fatal_error_callback_ = {});

    static ThreadGroupPtr createForBackgroundProcess(ContextPtr storage_context);

    std::vector<UInt64> getInvolvedThreadIds() const;
    size_t getPeakThreadsUsage() const;

    void linkThread(UInt64 thread_id);
    void unlinkThread();

private:
    mutable std::mutex mutex;

    /// Set up at creation, no race when reading
    SharedData shared_data TSA_GUARDED_BY(mutex);

    /// Set of all thread ids which has been attached to the group
    std::unordered_set<UInt64> thread_ids TSA_GUARDED_BY(mutex);

    /// Count of simultaneously working threads
    size_t active_thread_count TSA_GUARDED_BY(mutex) = 0;

    /// Peak threads count in the group
    size_t peak_threads_usage TSA_GUARDED_BY(mutex) = 0;
};

/**
 * Since merge is executed with multiple threads, this class
 * switches the parent MemoryTracker as part of the thread group to account all the memory used.
 */
class ThreadGroupSwitcher : private boost::noncopyable
{
public:
    explicit ThreadGroupSwitcher(ThreadGroupPtr thread_group);
    ~ThreadGroupSwitcher();

private:
    ThreadGroupPtr prev_thread_group;
};


/**
 * We use **constinit** here to tell the compiler the current_thread variable is initialized.
 * If we didn't help the compiler, then it would most likely add a check before every use of the variable to initialize it if needed.
 * Instead it will trust that we are doing the right thing (and we do initialize it to nullptr) and emit more optimal code.
 * This is noticeable in functions like CurrentMemoryTracker::free and CurrentMemoryTracker::allocImpl
 * See also:
 * - https://en.cppreference.com/w/cpp/language/constinit
 * - https://github.com/ClickHouse/ClickHouse/pull/40078
 */
extern thread_local constinit ThreadStatus * current_thread;

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
    /// Points to performance_counters by default.
    /// Could be changed to point to another object to calculate performance counters for some narrow scope.
    ProfileEvents::Counters * current_performance_counters{&performance_counters};

    MemoryTracker memory_tracker{VariableContext::Thread};
    /// Small amount of untracked memory (per thread atomic-less counter)
    Int64 untracked_memory = 0;
    /// Each thread could new/delete memory in range of (-untracked_memory_limit, untracked_memory_limit) without access to common counters.
    Int64 untracked_memory_limit = 4 * 1024 * 1024;

    /// Statistics of read and write rows/bytes
    Progress progress_in;
    Progress progress_out;

private:
    /// Group of threads, to which this thread attached
    ThreadGroupPtr thread_group;

    /// Is set once
    ContextWeakPtr global_context;
    /// Use it only from current thread
    ContextWeakPtr query_context;

    /// Is used to send logs from logs_queue to client in case of fatal errors.
    using FatalErrorCallback = std::function<void()>;
    FatalErrorCallback fatal_error_callback;

    ThreadGroup::SharedData local_data;

    bool performance_counters_finalized = false;

    String query_id_from_query_context;

    struct TimePoint
    {
        void setUp();
        UInt64 nanoseconds() const;
        UInt64 microseconds() const;
        UInt64 seconds() const;

        std::chrono::time_point<std::chrono::system_clock> point;
    };

    TimePoint query_start_time{};

    // CPU and Real time query profilers
    std::unique_ptr<QueryProfilerReal> query_profiler_real;
    std::unique_ptr<QueryProfilerCPU> query_profiler_cpu;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> taskstats;
    Stopwatch stopwatch{CLOCK_MONOTONIC_COARSE};
    UInt64 last_performance_counters_update_time = 0;

    /// See setInternalThread()
    bool internal_thread = false;

    /// This is helpful for cut linking dependencies for clickhouse_common_io
    using Deleter = std::function<void()>;
    Deleter deleter;

    LoggerPtr log = nullptr;

    bool check_current_thread_on_destruction;

public:
    explicit ThreadStatus(bool check_current_thread_on_destruction_ = true);
    ~ThreadStatus();

    ThreadGroupPtr getThreadGroup() const;

    const String & getQueryId() const;

    ContextPtr getQueryContext() const;
    ContextPtr getGlobalContext() const;

    /// "Internal" ThreadStatus is used for materialized views for separate
    /// tracking into system.query_views_log
    ///
    /// You can have multiple internal threads, but only one non-internal with
    /// the same thread_id.
    ///
    /// "Internal" thread:
    /// - cannot have query profiler
    ///   since the running (main query) thread should already have one
    /// - should not try to obtain latest counter on detach
    ///   because detaching of such threads will be done from a different
    ///   thread_id, and some counters are not available (i.e. getrusage()),
    ///   but anyway they are accounted correctly in the main ThreadStatus of a
    ///   query.
    void setInternalThread();

    /// Attaches slave thread to existing thread group
    void attachToGroup(const ThreadGroupPtr & thread_group_, bool check_detached = true);

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachFromGroup();

    /// Returns pointer to the current profile counters to restore them back.
    /// Note: consequent call with new scope will detach previous scope.
    ProfileEvents::Counters * attachProfileCountersScope(ProfileEvents::Counters * performance_counters_scope);

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                     LogsLevel client_logs_level);
    InternalTextLogsQueuePtr getInternalTextLogsQueue() const;
    LogsLevel getClientLogsLevel() const;

    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);
    InternalProfileEventsQueuePtr getInternalProfileEventsQueue() const;

    void attachQueryForLog(const String & query_);
    const String & getQueryForLog() const;

    bool isQueryCanceled() const;

    /// Proper cal for fatal_error_callback
    void onFatalError();

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();
    void updatePerformanceCountersIfNeeded();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Set the counters last usage to now
    void resetPerformanceCountersLastUsage();

    void logToQueryViewsLog(const ViewRuntimeData & vinfo);

    void flushUntrackedMemory();

private:
    void applyGlobalSettings();
    void applyQuerySettings();

    void initPerformanceCounters();

    void initQueryProfiler();

    void finalizeQueryProfiler();

    void logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database);

    void attachToGroupImpl(const ThreadGroupPtr & thread_group_);
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
