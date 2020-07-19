#pragma once

#include <memory>
#include <string>

#include <common/StringRef.h>
#include <Common/ThreadStatus.h>


namespace ProfileEvents
{
class Counters;
}

class MemoryTracker;


namespace DB
{

class Context;
class QueryStatus;
struct Progress;
class InternalTextLogsQueue;


/** Collection of static methods to work with thread-local objects.
  * Allows to attach and detach query/process (thread group) to a thread
  * (to calculate query-related metrics and to allow to obtain query-related data from a thread).
  * Thread will propagate it's metrics to attached query.
  */
class CurrentThread
{
public:
    /// Return true in case of successful initializaiton
    static bool isInitialized();

    /// Handler to current thread
    static ThreadStatus & get();

    /// Group to which belongs current thread
    static ThreadGroupStatusPtr getGroup();

    /// A logs queue used by TCPHandler to pass logs to a client
    static void attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
                                            LogsLevel client_logs_level);
    static std::shared_ptr<InternalTextLogsQueue> getInternalTextLogsQueue();

    static void setFatalErrorCallback(std::function<void()> callback);

    /// Makes system calls to update ProfileEvents that contain info from rusage and taskstats
    static void updatePerformanceCounters();

    static ProfileEvents::Counters & getProfileEvents();
    static MemoryTracker * getMemoryTracker();

    /// Update read and write rows (bytes) statistics (used in system.query_thread_log)
    static void updateProgressIn(const Progress & value);
    static void updateProgressOut(const Progress & value);

    /// Query management:

    /// Call from master thread as soon as possible (e.g. when thread accepted connection)
    static void initializeQuery();

    /// Sets query_context for current thread group
    static void attachQueryContext(Context & query_context);

    /// You must call one of these methods when create a query child thread:
    /// Add current thread to a group associated with the thread group
    static void attachTo(const ThreadGroupStatusPtr & thread_group);
    /// Is useful for a ThreadPool tasks
    static void attachToIfDetached(const ThreadGroupStatusPtr & thread_group);

    /// Update ProfileEvents and dumps info to system.query_thread_log
    static void finalizePerformanceCounters();

    /// Returns a non-empty string if the thread is attached to a query
    static StringRef getQueryId()
    {
        if (unlikely(!current_thread))
            return {};
        return current_thread->getQueryId();
    }

    /// Non-master threads call this method in destructor automatically
    static void detachQuery();
    static void detachQueryIfNotDetached();

    /// Initializes query with current thread as master thread in constructor, and detaches it in destructor
    struct QueryScope
    {
        explicit QueryScope(Context & query_context);
        ~QueryScope();

        void logPeakMemoryUsage();
        bool log_peak_memory_usage_in_destructor = true;
    };

private:
    static void defaultThreadDeleter();
};

}
