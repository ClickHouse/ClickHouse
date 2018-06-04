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
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


extern thread_local ThreadStatusPtr current_thread;


class ThreadStatus : public std::enable_shared_from_this<ThreadStatus>
{
public:

    /// Poco's thread number (the same number is used in logs)
    UInt32 thread_number = 0;
    /// Linux's PID (or TGID) (the same id is shown by ps util)
    Int32 os_thread_id = -1;

    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;

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
            bool check_detached = true);

    void detachQuery(bool thread_exits = false);

    void logToQueryThreadLog(QueryThreadLog & thread_log);

    void updatePerfomanceCountersImpl();

    std::atomic<int> thread_state{ThreadState::DetachedFromQuery};

    std::mutex mutex;
    QueryStatus * parent_query = nullptr;

    /// Is set once
    std::atomic<Context *> global_context{nullptr};
    /// Use it only from current thread
    Context * query_context = nullptr;

    UInt64 query_start_time_nanoseconds = 0;
    time_t query_start_time = 0;

    bool log_to_query_thread_log = true;
    bool log_profile_events = true;
    size_t queries_started = 0;

    Poco::Logger * log = nullptr;

    friend class CurrentThread;
    friend struct TasksStatsCounters;

    /// Use ptr to not add extra dependencies in header
    std::unique_ptr<RusageCounters> last_rusage;
    std::unique_ptr<TasksStatsCounters> last_taskstats;
    std::unique_ptr<TaskStatsInfoGetter> taskstats_getter;

public:
    class CurrentThreadScope;
};

}
