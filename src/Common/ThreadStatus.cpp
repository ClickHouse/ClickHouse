#include <sstream>

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

void ThreadStatus::setFatalErrorCallback(std::function<void()> callback)
{
    fatal_error_callback = std::move(callback);

    if (!thread_group)
        return;

    std::lock_guard lock(thread_group->mutex);
    thread_group->fatal_error_callback = fatal_error_callback;
}

void ThreadStatus::onFatalError()
{
    if (fatal_error_callback)
        fatal_error_callback();
}

}
