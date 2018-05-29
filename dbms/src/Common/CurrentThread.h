#pragma once
#include <memory>


namespace ProfileEvents
{
    struct Counters;
}

class MemoryTracker;


namespace DB
{

class QueryStatus;
class ThreadStatus;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;


class CurrentThread
{
public:

    static ThreadStatusPtr get();

    /// You must call one of these methods when create a child thread:

    /// Bundles the current thread with a query
    static void attachQuery(QueryStatus * parent_process);
    /// Bundles the current thread with a query bundled to the sibling thread
    static void attachQueryFromSiblingThread(const ThreadStatusPtr & sibling_thread);
    /// Is useful for a ThreadPool tasks
    static void attachQueryFromSiblingThreadIfDetached(const ThreadStatusPtr & sibling_thread);

    static void updatePerformanceCounters();
    static bool isAttachedToQuery();
    static ProfileEvents::Counters & getProfileEvents();
    static MemoryTracker & getMemoryTracker();

    /// Non-master threads call these method in destructor automatically
    static void detachQuery();

private:
    static void attachQueryFromSiblingThreadImpl(const ThreadStatusPtr & sibling_thread, bool check_detached = true);
};

}


