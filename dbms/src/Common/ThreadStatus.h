#pragma once

#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>

#include <IO/Progress.h>

#include <memory>
#include <map>
#include <mutex>
#include <shared_mutex>
#include <functional>
#include <boost/noncopyable.hpp>


namespace Poco
{
    class Logger;
}


namespace DB
{

class Context;
class QueryStatus;
class ThreadStatus;
class QueryThreadLog;
struct TasksStatsCounters;
struct RUsageCounters;
class TaskStatsInfoGetter;
class InternalTextLogsQueue;
using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;


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
    mutable std::mutex mutex;

    ProfileEvents::Counters performance_counters{VariableContext::Process};
    MemoryTracker memory_tracker{VariableContext::Process};

    Context * query_context = nullptr;
    Context * global_context = nullptr;

    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    std::vector<UInt32> thread_numbers;

    /// The first thread created this thread group
    UInt32 master_thread_number = 0;
    Int32 master_thread_os_id = -1;

    String query;
};

using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;


extern thread_local ThreadStatus * current_thread;

/** Encapsulates all per-thread info (ProfileEvents, MemoryTracker, query_id, query context, etc.).
  * The object must be created in thread function and destroyed in the same thread before the exit.
  * It is accessed through thread-local pointer.
  *
  * This object should be used only via "CurrentThread", see CurrentThread.h
  */
class ThreadStatus : public boost::noncopyable
{
public:
    ThreadStatus();
    ~ThreadStatus();

    /// Poco's thread number (the same number is used in logs)
    UInt32 thread_number = 0;
    /// Linux's PID (or TGID) (the same id is shown by ps util)
    Int32 os_thread_id = -1;

    /// TODO: merge them into common entity
    ProfileEvents::Counters performance_counters{VariableContext::Thread};
    MemoryTracker memory_tracker{VariableContext::Thread};

    /// Statistics of read and write rows/bytes
    Progress progress_in;
    Progress progress_out;

    using Deleter = std::function<void()>;
    Deleter deleter;

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

    const std::string & getQueryId() const;

    /// Starts new query and create new thread group for it, current thread becomes master thread of the query
    void initializeQuery();

    /// Attaches slave thread to existing thread group
    void attachQuery(const ThreadGroupStatusPtr & thread_group_, bool check_detached = true);

    InternalTextLogsQueuePtr getInternalTextLogsQueue() const
    {
        return thread_state == Died ? nullptr : logs_queue_ptr.lock();
    }

    void attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue);

    /// Sets query context for current thread and its thread group
    /// NOTE: query_context have to be alive until detachQuery() is called
    void attachQueryContext(Context & query_context);

    /// Update several ProfileEvents counters
    void updatePerformanceCounters();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    void finalizePerformanceCounters();

    /// Detaches thread from the thread group and the query, dumps performance counters if they have not been dumped
    void detachQuery(bool exit_if_already_detached = false, bool thread_exits = false);

protected:
    void initPerformanceCounters();

    void logToQueryThreadLog(QueryThreadLog & thread_log);

    void assertState(const std::initializer_list<int> & permitted_states, const char * description = nullptr);

    ThreadGroupStatusPtr thread_group;

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    /// Is set once
    Context * global_context = nullptr;
    /// Use it only from current thread
    Context * query_context = nullptr;

    String query_id;

    /// A logs queue used by TCPHandler to pass logs to a client
    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    bool performance_counters_finalized = false;
    UInt64 query_start_time_nanoseconds = 0;
    time_t query_start_time = 0;
    size_t queries_started = 0;

    Poco::Logger * log = nullptr;

    friend class CurrentThread;
    friend struct TasksStatsCounters;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RUsageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> last_taskstats;

    /// Set to non-nullptr only if we have enough capabilities.
    std::unique_ptr<TaskStatsInfoGetter> taskstats_getter;
};

}
