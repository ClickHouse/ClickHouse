#pragma once
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <IO/Progress.h>
#include <memory>
#include <mutex>


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
struct RusageCounters;
class TaskStatsInfoGetter;
class InternalTextLogsQueue;
using InternalTextLogsQueuePtr = std::shared_ptr<InternalTextLogsQueue>;
using InternalTextLogsQueueWeakPtr = std::weak_ptr<InternalTextLogsQueue>;


using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;
extern thread_local ThreadStatusPtr current_thread;


class ThreadStatus : public std::enable_shared_from_this<ThreadStatus>
{
public:

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

public:

    static ThreadStatusPtr create();

    /// Called by master thread when the query finishes
    void clean();

    enum ThreadState
    {
        DetachedFromQuery = 0,  /// We just created thread or it is background thread
        QueryInitializing,      /// We accepted a connection, but haven't enqueued a query to ProcessList
        AttachedToQuery,        /// Thread executes enqueued query
        Died,                   /// Thread does not exist
    };

    int getCurrentState() const
    {
        return thread_state.load(std::memory_order_relaxed);
    }

    InternalTextLogsQueuePtr getInternalTextLogsQueue() const
    {
        return thread_state == Died ? nullptr : logs_queue_ptr.lock();
    }

    void attachSystemLogsQueue(const InternalTextLogsQueuePtr & logs_queue)
    {
        std::lock_guard lock(mutex);
        logs_queue_ptr = logs_queue;
    }

    Context * getGlobalContext()
    {
        return global_context.load(std::memory_order_relaxed);
    }

    ~ThreadStatus();

protected:

    ThreadStatus();

    void initializeQuery();

    void attachQuery(
            QueryStatus * parent_query_,
            ProfileEvents::Counters * parent_counters,
            MemoryTracker * parent_memory_tracker,
            const InternalTextLogsQueueWeakPtr & logs_queue_ptr_,
            bool check_detached = true);

    void detachQuery(bool exit_if_already_detached = false, bool thread_exits = false);

    void logToQueryThreadLog(QueryThreadLog & thread_log);

    void updatePerformanceCountersImpl();

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    mutable std::mutex mutex;
    QueryStatus * parent_query = nullptr;

    /// Is set once
    std::atomic<Context *> global_context{nullptr};
    /// Use it only from current thread
    Context * query_context = nullptr;

    /// A logs queue used by TCPHandler to pass logs to a client
    InternalTextLogsQueueWeakPtr logs_queue_ptr;

    UInt64 query_start_time_nanoseconds = 0;
    time_t query_start_time = 0;

    bool log_to_query_thread_log = true;
    bool log_profile_events = true;
    size_t queries_started = 0;

    Poco::Logger * log = nullptr;

    friend class CurrentThread;
    friend struct TasksStatsCounters;

    /// Use ptr not to add extra dependencies in the header
    std::unique_ptr<RusageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> last_taskstats;
    std::unique_ptr<TaskStatsInfoGetter> taskstats_getter;
    bool has_permissions_for_taskstats = false;

public:
    class CurrentThreadScope;
};

}
