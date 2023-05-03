#pragma once

#include <Interpreters/Context_fwd.h>
#include <Common/ThreadStatus.h>

#include <memory>
#include <string>
#include <string_view>


namespace ProfileEvents
{
class Counters;
}

class MemoryTracker;


namespace DB
{

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
    /// Return true in case of successful initialization
    static bool isInitialized();

    /// Handler to current thread
    static ThreadStatus & get();

    /// Group to which belongs current thread
    static ThreadGroupStatusPtr getGroup();

    /// A logs queue used by TCPHandler to pass logs to a client
    static void attachInternalTextLogsQueue(const std::shared_ptr<InternalTextLogsQueue> & logs_queue,
                                            LogsLevel client_logs_level);
    static std::shared_ptr<InternalTextLogsQueue> getInternalTextLogsQueue();

    static void attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & queue);
    static InternalProfileEventsQueuePtr getInternalProfileEventsQueue();

    static void attachQueryForLog(const String & query_);

    /// Makes system calls to update ProfileEvents that contain info from rusage and taskstats
    static void updatePerformanceCounters();
    static void updatePerformanceCountersIfNeeded();

    static ProfileEvents::Counters & getProfileEvents();
    inline ALWAYS_INLINE static MemoryTracker * getMemoryTracker()
    {
        if (unlikely(!current_thread))
            return nullptr;
        return &current_thread->memory_tracker;
    }

    /// Update read and write rows (bytes) statistics (used in system.query_thread_log)
    static void updateProgressIn(const Progress & value);
    static void updateProgressOut(const Progress & value);

    /// You must call one of these methods when create a query child thread:
    /// Add current thread to a group associated with the thread group
    static void attachToGroup(const ThreadGroupStatusPtr & thread_group);
    /// Is useful for a ThreadPool tasks
    static void attachToGroupIfDetached(const ThreadGroupStatusPtr & thread_group);

    /// Non-master threads call this method in destructor automatically
    static void detachFromGroupIfNotDetached();

    /// Update ProfileEvents and dumps info to system.query_thread_log
    static void finalizePerformanceCounters();

    /// Returns a non-empty string if the thread is attached to a query
    static std::string_view getQueryId();

    /// Initializes query with current thread as master thread in constructor, and detaches it in destructor
    struct QueryScope : private boost::noncopyable
    {
        explicit QueryScope(ContextMutablePtr query_context, std::function<void()> fatal_error_callback = {});
        explicit QueryScope(ContextPtr query_context, std::function<void()> fatal_error_callback = {});
        ~QueryScope();

        void logPeakMemoryUsage();
        bool log_peak_memory_usage_in_destructor = true;
    };
};

}
