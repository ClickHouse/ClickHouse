#include <sstream>

#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/QueryProfiler.h>
#include <Common/ThreadStatus.h>

#include <Poco/Logger.h>
#include <common/getThreadId.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


thread_local ThreadStatus * current_thread = nullptr;


ThreadStatus::ThreadStatus()
    : thread_id{getThreadId()}
{
    last_rusage = std::make_unique<RUsageCounters>();
    perf_events = std::make_unique<PerfEventsCounters>();

    memory_tracker.setDescription("(for thread)");
    log = &Poco::Logger::get("ThreadStatus");

    current_thread = this;

    /// NOTE: It is important not to do any non-trivial actions (like updating ProfileEvents or logging) before ThreadStatus is created
    /// Otherwise it could lead to SIGSEGV due to current_thread dereferencing
}

ThreadStatus::~ThreadStatus()
{
    try
    {
        if (untracked_memory > 0)
            memory_tracker.alloc(untracked_memory);
        else
            memory_tracker.free(-untracked_memory);
    }
    catch (const DB::Exception &)
    {
        /// It's a minor tracked memory leak here (not the memory itself but it's counter).
        /// We've already allocated a little bit more then the limit and cannot track it in the thread memory tracker or its parent.
    }

    if (deleter)
        deleter();
    current_thread = nullptr;
}

void ThreadStatus::initPerformanceCounters()
{
    performance_counters_finalized = false;

    /// Clear stats from previous query if a new query is started
    /// TODO: make separate query_thread_performance_counters and thread_performance_counters
    performance_counters.resetCounters();
    memory_tracker.resetCounters();
    memory_tracker.setDescription("(for thread)");

    query_start_time_nanoseconds = getCurrentTimeNanoseconds();
    query_start_time = time(nullptr);
    ++queries_started;

    *last_rusage = RUsageCounters::current(query_start_time_nanoseconds);

    try
    {
        PerfEventsCounters::initializeProfileEvents(*perf_events);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    if (!taskstats)
    {
        try
        {
            taskstats = TasksStatsCounters::create(thread_id);
        }
        catch (...)
        {
            tryLogCurrentException(log);
        }
    }
    if (taskstats)
        taskstats->reset();
}

void ThreadStatus::updatePerformanceCounters()
{
    try
    {
        RUsageCounters::updateProfileEvents(*last_rusage, performance_counters);
        if (taskstats)
            taskstats->updateCounters(performance_counters);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::assertState(const std::initializer_list<int> & permitted_states, const char * description) const
{
    for (auto permitted_state : permitted_states)
    {
        if (getCurrentState() == permitted_state)
            return;
    }

    std::stringstream ss;
    ss << "Unexpected thread state " << getCurrentState();
    if (description)
        ss << ": " << description;
    throw Exception(ss.str(), ErrorCodes::LOGICAL_ERROR);
}

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue,
                                               LogsLevel client_logs_level)
{
    logs_queue_ptr = logs_queue;

    if (!thread_group)
        return;

    std::lock_guard lock(thread_group->mutex);
    thread_group->logs_queue_ptr = logs_queue;
    thread_group->client_logs_level = client_logs_level;
}

}
