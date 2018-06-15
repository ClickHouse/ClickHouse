#pragma once
#include <memory>
#include <string>


namespace ProfileEvents
{
class Counters;
}

class MemoryTracker;


namespace DB
{

class QueryStatus;
class ThreadStatus;
struct Progress;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;
class InternalTextLogsQueue;


class CurrentThread
{
public:

    static ThreadStatusPtr get();

    /// Call when thread accepted connection (but haven't called executeQuery())
    /// Currently it is used only for debugging
    static void initializeQuery();

    /// A logs queue used by TCPHandler to pass logs to a client
    static void attachSystemLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue);
    static std::shared_ptr<InternalTextLogsQueue> getInternalTextLogsQueue();

    /// You must call one of these methods when create a child thread:
    /// Bundles the current thread with a query
    static void attachQuery(QueryStatus * parent_process);
    /// Bundles the current thread with a query bundled to the sibling thread
    static void attachQueryFromSiblingThread(const ThreadStatusPtr & sibling_thread);
    /// Is useful for a ThreadPool tasks
    static void attachQueryFromSiblingThreadIfDetached(const ThreadStatusPtr & sibling_thread);

    /// Makes system calls to update ProfileEvents derived from rusage and taskstats
    static void updatePerformanceCounters();

    static ProfileEvents::Counters & getProfileEvents();
    static MemoryTracker & getMemoryTracker();

    /// Returns a non-empty string if the thread is attached to a query
    static std::string getCurrentQueryID();

    static void updateProgressIn(const Progress & value);
    static void updateProgressOut(const Progress & value);

    /// Non-master threads call this method in destructor automatically
    static void detachQuery();
    static void detachQueryIfNotDetached();

private:
    static void attachQueryFromSiblingThreadImpl(ThreadStatusPtr sibling_thread, bool check_detached = true);
};

}


