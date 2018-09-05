#include <Common/ThreadStatus.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/ProcessList.h>


/// Implement some methods of ThreadStatus and CurrentThread here to avoid extra linking dependencies in clickhouse_common_io
namespace DB
{

void ThreadStatus::attachQueryContext(Context & query_context_)
{
    query_context = &query_context_;
    if (!global_context)
        global_context = &query_context->getGlobalContext();

    if (!thread_group)
        return;

    std::unique_lock lock(thread_group->mutex);
    thread_group->query_context = query_context;
    if (!thread_group->global_context)
        thread_group->global_context = global_context;
}

String ThreadStatus::getQueryID()
{
    if (query_context)
        return query_context->getClientInfo().current_query_id;

    return {};
}

void ThreadStatus::defaultThreadDeleter()
{
    ThreadStatus & thread = *CurrentThread::get();
    LOG_TRACE(thread.log, "Thread " << thread.thread_number << " exited");
    thread.detachQuery(true, true);
}

void ThreadStatus::initializeQuery()
{
    assertState({ThreadState::DetachedFromQuery}, __PRETTY_FUNCTION__);

    thread_group = std::make_shared<ThreadGroupStatus>();

    performance_counters.setParent(&thread_group->performance_counters);
    memory_tracker.setParent(&thread_group->memory_tracker);
    thread_group->memory_tracker.setDescription("(for query)");

    thread_group->master_thread = shared_from_this();
    thread_group->thread_statuses.emplace(thread_number, shared_from_this());

    initPerformanceCounters();
    thread_state = ThreadState::AttachedToQuery;
    current_thread_scope.deleter = ThreadStatus::defaultThreadDeleter;
}

void ThreadStatus::attachQuery(const ThreadGroupStatusPtr & thread_group_, bool check_detached)
{
    if (thread_state == ThreadState::AttachedToQuery)
    {
        if (check_detached)
            throw Exception("Can't attach query to the thread, it is already attached", ErrorCodes::LOGICAL_ERROR);
        return;
    }

    assertState({ThreadState::DetachedFromQuery}, __PRETTY_FUNCTION__);

    if (!thread_group_)
        throw Exception("Attempt to attach to nullptr thread group", ErrorCodes::LOGICAL_ERROR);

    /// Attach current thread to thread group and copy useful information from it
    thread_group = thread_group_;

    performance_counters.setParent(&thread_group->performance_counters);
    memory_tracker.setParent(&thread_group->memory_tracker);

    {
        std::unique_lock lock(thread_group->mutex);

        logs_queue_ptr = thread_group->logs_queue_ptr;
        query_context = thread_group->query_context;

        if (!global_context)
            global_context = thread_group->global_context;

        if (!thread_group->thread_statuses.emplace(thread_number, shared_from_this()).second)
            throw Exception("Thread " + std::to_string(thread_number) + " is attached twice", ErrorCodes::LOGICAL_ERROR);
    }

    initPerformanceCounters();
    thread_state = ThreadState::AttachedToQuery;
    current_thread_scope.deleter = ThreadStatus::defaultThreadDeleter;
}

void ThreadStatus::finalizePerformanceCounters()
{
    if (performance_counters_finalized)
        return;

    performance_counters_finalized = true;
    updatePerformanceCounters();

    try
    {
        bool log_to_query_thread_log = global_context && query_context && query_context->getSettingsRef().log_query_threads.value != 0;
        if (log_to_query_thread_log)
            if (auto thread_log = global_context->getQueryThreadLog())
                logToQueryThreadLog(*thread_log);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::detachQuery(bool exit_if_already_detached, bool thread_exits)
{
    if (exit_if_already_detached && thread_state == ThreadState::DetachedFromQuery)
    {
        thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
        return;
    }

    assertState({ThreadState::AttachedToQuery}, __PRETTY_FUNCTION__);
    finalizePerformanceCounters();

    /// For better logging ({query_id} will be shown here)
    if (thread_group && thread_group.use_count() == 1)
        thread_group->memory_tracker.logPeakMemoryUsage();

    /// Detach from thread group
    performance_counters.setParent(&ProfileEvents::global_counters);
    memory_tracker.setParent(nullptr);
    query_context = nullptr;
    thread_group.reset();

    thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
}

void ThreadStatus::logToQueryThreadLog(QueryThreadLog & thread_log)
{
    QueryThreadLogElement elem;

    elem.event_time = time(nullptr);
    elem.query_start_time = query_start_time;
    elem.query_duration_ms = (getCurrentTimeNanoseconds() - query_start_time_nanoseconds) / 1000000U;

    elem.read_rows = progress_in.rows.load(std::memory_order_relaxed);
    elem.read_bytes = progress_in.bytes.load(std::memory_order_relaxed);
    elem.written_rows = progress_out.rows.load(std::memory_order_relaxed);
    elem.written_bytes = progress_out.bytes.load(std::memory_order_relaxed);
    elem.memory_usage = memory_tracker.get();
    elem.peak_memory_usage = memory_tracker.getPeak();

    elem.thread_name = getThreadName();
    elem.thread_number = thread_number;
    elem.os_thread_id = os_thread_id;

    if (thread_group)
    {
        {
            std::shared_lock lock(thread_group->mutex);

            if (thread_group->master_thread)
            {
                elem.master_thread_number = thread_group->master_thread->thread_number;
                elem.master_os_thread_id = thread_group->master_thread->os_thread_id;
            }

            elem.query = thread_group->query;
        }
    }

    if (query_context)
    {
        elem.client_info = query_context->getClientInfo();

        if (query_context->getSettingsRef().log_profile_events.value != 0)
        {
            /// NOTE: Here we are in the same thread, so we can make memcpy()
            elem.profile_counters = std::make_shared<ProfileEvents::Counters>(performance_counters.getPartiallyAtomicSnapshot());
        }
    }

    thread_log.add(elem);
}


void CurrentThread::initializeQuery()
{
    get()->initializeQuery();
}

void CurrentThread::attachTo(const ThreadGroupStatusPtr & thread_group)
{
    get()->attachQuery(thread_group, true);
}

void CurrentThread::attachToIfDetached(const ThreadGroupStatusPtr & thread_group)
{
    get()->attachQuery(thread_group, false);
}

std::string CurrentThread::getCurrentQueryID()
{
    if (!current_thread || current_thread.use_count() <= 0)
        return {};

    return current_thread->getQueryID();
}

void CurrentThread::attachQueryContext(Context & query_context)
{
    return get()->attachQueryContext(query_context);
}

void CurrentThread::finalizePerformanceCounters()
{
    get()->finalizePerformanceCounters();
}

void CurrentThread::detachQuery()
{
    get()->detachQuery(false);
}

void CurrentThread::detachQueryIfNotDetached()
{
    get()->detachQuery(true);
}


CurrentThread::QueryScope::QueryScope(Context & query_context)
{
    CurrentThread::initializeQuery();
    CurrentThread::attachQueryContext(query_context);
}

CurrentThread::QueryScope::~QueryScope()
{
    try
    {
        CurrentThread::detachQueryIfNotDetached();
    }
    catch (...)
    {
        tryLogCurrentException("CurrentThread", __PRETTY_FUNCTION__);
    }
}

}
