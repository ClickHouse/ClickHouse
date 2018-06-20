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

class Context;
class QueryStatus;
class ThreadStatus;
struct Progress;
using ThreadStatusPtr = std::shared_ptr<ThreadStatus>;
class InternalTextLogsQueue;
class ThreadGroupStatus;
using ThreadGroupStatusPtr = std::shared_ptr<ThreadGroupStatus>;


class CurrentThread
{
public:

    static ThreadStatusPtr get();
    static ThreadGroupStatusPtr getGroup();

    /// Call from master thread as soon as possible (e.g. when thread accepted connection)
    static void initializeQuery();

    /// A logs queue used by TCPHandler to pass logs to a client
    static void attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue);
    static std::shared_ptr<InternalTextLogsQueue> getInternalTextLogsQueue();

    /// Sets query_context for current thread group
    static void attachQueryContext(Context & query_context);

    /// You must call one of these methods when create a child thread:
    /// Bundles the current thread with a query bundled to the sibling thread
    static void attachTo(const ThreadGroupStatusPtr & thread_group);
    /// Is useful for a ThreadPool tasks
    static void attachToIfDetached(const ThreadGroupStatusPtr & thread_group);

    /// Makes system calls to update ProfileEvents derived from rusage and taskstats
    static void updatePerformanceCounters();
    /// Update ProfileEvents and dumps info to system.query_thread_log
    static void finalizePerformanceCounters();

    static ProfileEvents::Counters & getProfileEvents();
    static MemoryTracker & getMemoryTracker();

    /// Returns a non-empty string if the thread is attached to a query
    static std::string getCurrentQueryID();

    static void updateProgressIn(const Progress & value);
    static void updateProgressOut(const Progress & value);

    /// Non-master threads call this method in destructor automatically
    static void detachQuery();
    static void detachQueryIfNotDetached();


    /// Initializes query with current thread as master thread in constructor, and detaches it in desstructor
    struct QueryScope
    {
        explicit QueryScope(Context & query_context)
        {
            CurrentThread::initializeQuery();
            CurrentThread::attachQueryContext(query_context);
        }

        ~QueryScope();
    };
};

}

