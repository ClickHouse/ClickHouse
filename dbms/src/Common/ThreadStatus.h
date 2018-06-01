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
class ScopeCurrentThread;
class QueryThreadLog;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


extern thread_local ThreadStatusPtr current_thread;


class ThreadStatus : public std::enable_shared_from_this<ThreadStatus>
{
public:

    UInt32 poco_thread_number = 0;
    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;
    Int32 os_thread_id = -1;

    Progress progress_in;
    Progress progress_out;

public:

    static ThreadStatusPtr create();

    /// Called by master thread when the query finishes
    void reset();

    QueryStatus * getParentQuery()
    {
        return parent_query.load(std::memory_order_relaxed);
    }

    Context * getGlobalContext()
    {
        return global_context.load(std::memory_order_relaxed);
    }

    Context * getQueryContext()
    {
        return query_context.load(std::memory_order_relaxed);
    }

    ~ThreadStatus();

protected:

    ThreadStatus();

    void attachQuery(
            QueryStatus * parent_query_,
            ProfileEvents::Counters * parent_counters,
            MemoryTracker *parent_memory_tracker,
            bool check_detached = true);

    void detachQuery(bool thread_exits = false);

    void logToQueryThreadLog(QueryThreadLog & thread_log);

    void updatePerfomanceCountersImpl();

    std::mutex mutex;
    std::atomic<bool> is_active_query{false};
    bool is_active_thread{false};
    bool is_first_query_of_the_thread{true};

    UInt64 query_start_time_nanoseconds{0};
    time_t query_start_time{0};

    std::atomic<QueryStatus *> parent_query{nullptr};
    /// Use it only from current thread
    std::atomic<Context *> query_context{nullptr};
    /// Is set once
    std::atomic<Context *> global_context{nullptr};

    bool log_to_query_thread_log = true;
    bool log_profile_events = true;

    Poco::Logger * log = nullptr;

    friend class CurrentThread;
    friend struct TasksStatsCounters;

    struct Impl;
    std::unique_ptr<Impl> impl;

public:
    class CurrentThreadScope;
};

}
