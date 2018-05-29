#pragma once
#include <Common/ProfileEvents.h>
#include <Common/MemoryTracker.h>
#include <memory>
#include <mutex>


namespace Poco
{
    class Logger;
}


namespace DB
{

class QueryStatus;
class ThreadStatus;
class ScopeCurrentThread;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


extern thread_local ThreadStatusPtr current_thread;


class ThreadStatus
{
public:

    UInt32 poco_thread_number = 0;
    QueryStatus * parent_query = nullptr;
    ProfileEvents::Counters performance_counters;
    MemoryTracker memory_tracker;
    int os_thread_id = -1;
    std::mutex mutex;

public:

    /// A constructor
    static ThreadStatusPtr create();

    /// Reset all references and metrics
    void reset();

    ~ThreadStatus();

protected:

    ThreadStatus();
    void attachQuery(
            QueryStatus *parent_query_,
            ProfileEvents::Counters *parent_counters,
            MemoryTracker *parent_memory_tracker,
            bool check_detached = true);
    void detachQuery();
    void updatePerfomanceCountersImpl();

    bool is_active_query = false;
    bool is_active_thread = false;
    bool is_first_query_of_the_thread = true;
    Poco::Logger * log;

    friend class CurrentThreadScope;
    friend class CurrentThread;
    friend struct TasksStatsCounters;

    struct Impl;
    std::unique_ptr<Impl> impl;
    Impl & getImpl() { return *impl; }
};

}
