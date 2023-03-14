#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <IO/Progress.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
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
class ThreadGroupStatus;
using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;

class ThreadGroupStatus
{
public:
    /// The first thread created this thread group
    UInt64 master_thread_id = 0;

    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker memory_tracker{VariableContext::Process};

    /// Access to the members below has to be in critical section with  mutex
    mutable std::mutex mutex;

    InternalTextLogsQueueWeakPtr logs_queue_ptr;
    InternalProfileEventsQueueWeakPtr profile_queue_ptr;

    LogsLevel client_logs_level = LogsLevel::none;

    String query;
    UInt64 normalized_query_hash = 0;

    /// When new query starts, new thread group is created for it, current thread becomes master thread of the query
    static ThreadGroupStatusPtr createForQuery(ContextPtr query_context_, std::function<void()> fatal_error_callback_ = {});

    const std::vector<UInt64> getInvolvedThreadIds() const;

    void link(ThreadStatusPtr thread);
    void unlink(ThreadStatusPtr thread);

    ContextWeakPtr getQueryContextWeak() const;
    ContextWeakPtr getGlobalContextWeak() const;

    using FatalErrorCallback = std::function<void()>;
    FatalErrorCallback getFatalErrorCallback() const;

private:
    /// Set up at creation, no race when reading
    ContextWeakPtr query_context;
    ContextWeakPtr global_context;

    /// Set up at creation, no race when reading
    FatalErrorCallback fatal_error_callback;

    /// Set of all thread ids which has been attached to the group
    std::unordered_set<UInt64> thread_ids;

    /// Set of active threads
    std::unordered_set<ThreadStatusPtr> threads;
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

protected:
    /// Group of threads, to which this thread attached
    ThreadGroupStatusPtr thread_group;

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    /// Is set once
    ContextWeakPtr global_context;
    /// Use it only from current thread
    ContextWeakPtr query_context;

    String query_id_from_query_context;

    /// A logs queue used by TCPHandler to pass logs to a client
    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    InternalProfileEventsQueueWeakPtr profile_queue_ptr;

    struct TimePoint
    {
        void setUp();
        void SetUp(std::chrono::time_point<std::chrono::system_clock> now);

        UInt64 nanoseconds = 0;
        UInt64 microseconds = 0;
        time_t seconds = 0;
    };

    bool performance_counters_finalized = false;
    TimePoint query_start_time{};

    // CPU and Real time query profilers
    std::unique_ptr<QueryProfilerReal> query_profiler_real;
    std::unique_ptr<QueryProfilerCPU> query_profiler_cpu;

    Poco::Logger * log = nullptr;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> taskstats;

    /// Is used to send logs from logs_queue to client in case of fatal errors.
    std::function<void()> fatal_error_callback;

    /// See setInternalThread()
    bool internal_thread = false;

    /// Requires access to query_id.
    friend class MemoryTrackerThreadSwitcher;
    void setQueryId(const String & query_id_)
    {
        query_id_from_query_context = query_id_;
    }

    /// This is helpful for cut linking dependencies for clickhouse_common_io
    using Deleter = std::function<void()>;
    Deleter deleter;

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
    };

    std::string_view getQueryId() const
    {
        return query_id_from_query_context;
    }

    auto getQueryContext() const
    {
        return query_context.lock();
    }

    auto getGlobalContext() const
    {
        return global_context.lock();
    }

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
    void attachTo(const ThreadGroupStatusPtr & thread_group_, bool check_detached = true);

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachGroup();

    /// Returns pointer to the current profile counters to restore them back.
    /// Note: consequent call with new scope will detach previous scope.
    ProfileEvents::Counters * attachProfileCountersScope(ProfileEvents::Counters * performance_counters_scope);

    InternalTextLogsQueuePtr getInternalTextLogsQueue() const
    {
        return logs_queue_ptr.lock();
    }

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                     LogsLevel client_logs_level);

    InternalProfileEventsQueuePtr getInternalProfileEventsQueue() const
    {
        return profile_queue_ptr.lock();
    }

    void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue);

    /// Proper cal for fatal_error_callback
    void onFatalError();

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Set the counters last usage to now
    void resetPerformanceCountersLastUsage();

    void logToQueryViewsLog(const ViewRuntimeData & vinfo);

    void flushUntrackedMemory();

protected:
    void applyQuerySettings();

    void initPerformanceCounters();

    void initQueryProfiler();

    void finalizeQueryProfiler();

    void logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database, std::chrono::time_point<std::chrono::system_clock> now);

    void assertState(ThreadState permitted_state, const char * description = nullptr) const;

private:
    void attachGroupImp(const ThreadGroupStatusPtr & thread_group_);
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
