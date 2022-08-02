#include <mutex>
#include <Common/ThreadStatus.h>

#include <Processors/Transforms/buildPushingToViewsChain.h>
#include <Interpreters/Context.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/TraceCollector.h>
#include <Parsers/formatAST.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/QueryProfiler.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/noexcept_scope.h>
#include <base/errnoToString.h>

#if defined(OS_LINUX)
#   include <Common/hasLinuxCapability.h>

#   include <sys/time.h>
#   include <sys/resource.h>
#endif


/// Implement some methods of ThreadStatus and CurrentThread here to avoid extra linking dependencies in clickhouse_common_io
/// TODO It doesn't make sense.

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_SET_THREAD_PRIORITY;
}

void ThreadStatus::applyQuerySettings()
{
    auto query_context_ptr = query_context.lock();
    assert(query_context_ptr);
    const Settings & settings = query_context_ptr->getSettingsRef();

    query_id = query_context_ptr->getCurrentQueryId();
    initQueryProfiler();

    untracked_memory_limit = settings.max_untracked_memory;
    if (settings.memory_profiler_step && settings.memory_profiler_step < static_cast<UInt64>(untracked_memory_limit))
        untracked_memory_limit = settings.memory_profiler_step;

#if defined(OS_LINUX)
    /// Set "nice" value if required.
    Int32 new_os_thread_priority = settings.os_thread_priority;
    if (new_os_thread_priority && hasLinuxCapability(CAP_SYS_NICE))
    {
        LOG_TRACE(log, "Setting nice to {}", new_os_thread_priority);

        if (0 != setpriority(PRIO_PROCESS, thread_id, new_os_thread_priority))
            throwFromErrno("Cannot 'setpriority'", ErrorCodes::CANNOT_SET_THREAD_PRIORITY);

        os_thread_priority = new_os_thread_priority;
    }
#endif
}


void ThreadStatus::attachQueryContext(ContextPtr query_context_)
{
    query_context = query_context_;

    if (global_context.expired())
        global_context = query_context_->getGlobalContext();

    if (thread_group)
    {
        std::lock_guard lock(thread_group->mutex);

        thread_group->query_context = query_context;
        if (thread_group->global_context.expired())
            thread_group->global_context = global_context;
    }

    applyQuerySettings();
}

void CurrentThread::defaultThreadDeleter()
{
    if (unlikely(!current_thread))
        return;
    current_thread->detachQuery(true, true);
}

void ThreadStatus::setupState(const ThreadGroupStatusPtr & thread_group_)
{
    assertState({ThreadState::DetachedFromQuery}, __PRETTY_FUNCTION__);

    /// Attach or init current thread to thread group and copy useful information from it
    thread_group = thread_group_;

    performance_counters.setParent(&thread_group->performance_counters);
    memory_tracker.setParent(&thread_group->memory_tracker);

    {
        std::lock_guard lock(thread_group->mutex);

        /// NOTE: thread may be attached multiple times if it is reused from a thread pool.
        thread_group->thread_ids.emplace_back(thread_id);
        thread_group->threads.insert(this);

        logs_queue_ptr = thread_group->logs_queue_ptr;
        fatal_error_callback = thread_group->fatal_error_callback;
        query_context = thread_group->query_context;
        profile_queue_ptr = thread_group->profile_queue_ptr;

        if (global_context.expired())
            global_context = thread_group->global_context;
    }

    if (auto query_context_ptr = query_context.lock())
    {
        applyQuerySettings();
    }

    initPerformanceCounters();

    thread_state = ThreadState::AttachedToQuery;
}

void ThreadStatus::initializeQuery()
{
    setupState(std::make_shared<ThreadGroupStatus>());

    /// No need to lock on mutex here
    thread_group->memory_tracker.setDescription("(for query)");
    thread_group->master_thread_id = thread_id;
}

void ThreadStatus::attachQuery(const ThreadGroupStatusPtr & thread_group_, bool check_detached)
{
    if (thread_state == ThreadState::AttachedToQuery)
    {
        if (check_detached)
            throw Exception("Can't attach query to the thread, it is already attached", ErrorCodes::LOGICAL_ERROR);
        return;
    }

    if (!thread_group_)
        throw Exception("Attempt to attach to nullptr thread group", ErrorCodes::LOGICAL_ERROR);

    setupState(thread_group_);
}

inline UInt64 time_in_nanoseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}


inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

void ThreadStatus::initPerformanceCounters()
{
    performance_counters_finalized = false;

    /// Clear stats from previous query if a new query is started
    /// TODO: make separate query_thread_performance_counters and thread_performance_counters
    performance_counters.resetCounters();
    memory_tracker.resetCounters();
    memory_tracker.setDescription("(for thread)");

    // query_start_time_{microseconds, nanoseconds} are all constructed from the same time point
    // to ensure that they are all equal up to the precision of a second.
    const auto now = std::chrono::system_clock::now();

    query_start_time_nanoseconds = time_in_nanoseconds(now);
    query_start_time = time_in_seconds(now);
    query_start_time_microseconds = time_in_microseconds(now);
    ++queries_started;

    // query_start_time_nanoseconds cannot be used here since RUsageCounters expect CLOCK_MONOTONIC
    *last_rusage = RUsageCounters::current();

    if (auto query_context_ptr = query_context.lock())
    {
        const Settings & settings = query_context_ptr->getSettingsRef();
        if (settings.metrics_perf_events_enabled)
        {
            try
            {
                current_thread_counters.initializeProfileEvents(
                    settings.metrics_perf_events_list);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
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

void ThreadStatus::finalizePerformanceCounters()
{
    if (performance_counters_finalized)
        return;

    performance_counters_finalized = true;
    updatePerformanceCounters();

    // We want to close perf file descriptors if the perf events were enabled for
    // one query. What this code does in practice is less clear -- e.g., if I run
    // 'select 1 settings metrics_perf_events_enabled = 1', I still get
    // query_context->getSettingsRef().metrics_perf_events_enabled == 0 *shrug*.
    bool close_perf_descriptors = true;
    if (auto query_context_ptr = query_context.lock())
        close_perf_descriptors = !query_context_ptr->getSettingsRef().metrics_perf_events_enabled;

    try
    {
        current_thread_counters.finalizeProfileEvents(performance_counters);
        if (close_perf_descriptors)
            current_thread_counters.closeEventDescriptors();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    try
    {
        auto global_context_ptr = global_context.lock();
        auto query_context_ptr = query_context.lock();
        if (global_context_ptr && query_context_ptr)
        {
            const auto & settings = query_context_ptr->getSettingsRef();
            if (settings.log_queries && settings.log_query_threads)
            {
                const auto now = std::chrono::system_clock::now();
                Int64 query_duration_ms = (time_in_microseconds(now) - query_start_time_microseconds) / 1000;
                if (query_duration_ms >= settings.log_queries_min_query_duration_ms.totalMilliseconds())
                {
                    if (auto thread_log = global_context_ptr->getQueryThreadLog())
                        logToQueryThreadLog(*thread_log, query_context_ptr->getCurrentDatabase(), now);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::resetPerformanceCountersLastUsage()
{
    *last_rusage = RUsageCounters::current();
    if (taskstats)
        taskstats->reset();
}

void ThreadStatus::initQueryProfiler()
{
    if (!query_profiler_enabled)
        return;

    /// query profilers are useless without trace collector
    auto global_context_ptr = global_context.lock();
    if (!global_context_ptr || !global_context_ptr->hasTraceCollector())
        return;

    auto query_context_ptr = query_context.lock();
    assert(query_context_ptr);
    const auto & settings = query_context_ptr->getSettingsRef();

    try
    {
        if (settings.query_profiler_real_time_period_ns > 0)
            query_profiler_real = std::make_unique<QueryProfilerReal>(thread_id,
                /* period= */ static_cast<UInt32>(settings.query_profiler_real_time_period_ns));

        if (settings.query_profiler_cpu_time_period_ns > 0)
            query_profiler_cpu = std::make_unique<QueryProfilerCPU>(thread_id,
                /* period= */ static_cast<UInt32>(settings.query_profiler_cpu_time_period_ns));
    }
    catch (...)
    {
        /// QueryProfiler is optional.
        tryLogCurrentException("ThreadStatus", "Cannot initialize QueryProfiler");
    }
}

void ThreadStatus::finalizeQueryProfiler()
{
    query_profiler_real.reset();
    query_profiler_cpu.reset();
}

void ThreadStatus::detachQuery(bool exit_if_already_detached, bool thread_exits)
{
    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    if (exit_if_already_detached && thread_state == ThreadState::DetachedFromQuery)
    {
        thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
        return;
    }

    assertState({ThreadState::AttachedToQuery}, __PRETTY_FUNCTION__);

    finalizeQueryProfiler();
    finalizePerformanceCounters();

    /// Detach from thread group
    {
        std::lock_guard guard(thread_group->mutex);
        thread_group->threads.erase(this);
    }
    performance_counters.setParent(&ProfileEvents::global_counters);
    memory_tracker.reset();

    memory_tracker.setParent(thread_group->memory_tracker.getParent());

    query_id.clear();
    query_context.reset();
    thread_group.reset();

    thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;

#if defined(OS_LINUX)
    if (os_thread_priority)
    {
        LOG_TRACE(log, "Resetting nice");

        if (0 != setpriority(PRIO_PROCESS, thread_id, 0))
            LOG_ERROR(log, "Cannot 'setpriority' back to zero: {}", errnoToString(ErrorCodes::CANNOT_SET_THREAD_PRIORITY, errno));

        os_thread_priority = 0;
    }
#endif
}

void ThreadStatus::logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database, std::chrono::time_point<std::chrono::system_clock> now)
{
    QueryThreadLogElement elem;

    // construct current_time and current_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    auto current_time = time_in_seconds(now);
    auto current_time_microseconds = time_in_microseconds(now);

    elem.event_time = current_time;
    elem.event_time_microseconds = current_time_microseconds;
    elem.query_start_time = query_start_time;
    elem.query_start_time_microseconds = query_start_time_microseconds;
    elem.query_duration_ms = (time_in_nanoseconds(now) - query_start_time_nanoseconds) / 1000000U;

    elem.read_rows = progress_in.read_rows.load(std::memory_order_relaxed);
    elem.read_bytes = progress_in.read_bytes.load(std::memory_order_relaxed);

    elem.written_rows = progress_out.written_rows.load(std::memory_order_relaxed);
    elem.written_bytes = progress_out.written_bytes.load(std::memory_order_relaxed);
    elem.memory_usage = memory_tracker.get();
    elem.peak_memory_usage = memory_tracker.getPeak();

    elem.thread_name = getThreadName();
    elem.thread_id = thread_id;

    elem.current_database = current_database;
    if (thread_group)
    {
        {
            std::lock_guard lock(thread_group->mutex);

            elem.master_thread_id = thread_group->master_thread_id;
            elem.query = thread_group->query;
            elem.normalized_query_hash = thread_group->normalized_query_hash;
        }
    }

    auto query_context_ptr = query_context.lock();
    if (query_context_ptr)
    {
        elem.client_info = query_context_ptr->getClientInfo();

        if (query_context_ptr->getSettingsRef().log_profile_events != 0)
        {
            /// NOTE: Here we are in the same thread, so we can make memcpy()
            elem.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(performance_counters.getPartiallyAtomicSnapshot());
        }
    }

    thread_log.add(elem);
}

static String getCleanQueryAst(const ASTPtr q, ContextPtr context)
{
    String res = serializeAST(*q, true);
    if (auto * masker = SensitiveDataMasker::getInstance())
        masker->wipeSensitiveData(res);

    res = res.substr(0, context->getSettingsRef().log_queries_cut_to_length);

    return res;
}

void ThreadStatus::logToQueryViewsLog(const ViewRuntimeData & vinfo)
{
    auto query_context_ptr = query_context.lock();
    if (!query_context_ptr)
        return;
    auto views_log = query_context_ptr->getQueryViewsLog();
    if (!views_log)
        return;

    QueryViewsLogElement element;

    element.event_time = time_in_seconds(vinfo.runtime_stats->event_time);
    element.event_time_microseconds = time_in_microseconds(vinfo.runtime_stats->event_time);
    element.view_duration_ms = vinfo.runtime_stats->elapsed_ms;

    element.initial_query_id = query_id;
    element.view_name = vinfo.table_id.getFullTableName();
    element.view_uuid = vinfo.table_id.uuid;
    element.view_type = vinfo.runtime_stats->type;
    if (vinfo.query)
        element.view_query = getCleanQueryAst(vinfo.query, query_context_ptr);
    element.view_target = vinfo.runtime_stats->target_name;

    auto events = std::make_shared<ProfileEvents::Counters::Snapshot>(performance_counters.getPartiallyAtomicSnapshot());
    element.read_rows = progress_in.read_rows.load(std::memory_order_relaxed);
    element.read_bytes = progress_in.read_bytes.load(std::memory_order_relaxed);
    element.written_rows = progress_out.written_rows.load(std::memory_order_relaxed);
    element.written_bytes = progress_out.written_bytes.load(std::memory_order_relaxed);
    element.peak_memory_usage = memory_tracker.getPeak() > 0 ? memory_tracker.getPeak() : 0;
    if (query_context_ptr->getSettingsRef().log_profile_events != 0)
    {
        element.profile_counters = events;
    }

    element.status = vinfo.runtime_stats->event_status;
    element.exception_code = 0;
    if (vinfo.exception)
    {
        element.exception_code = getExceptionErrorCode(vinfo.exception);
        element.exception = getExceptionMessage(vinfo.exception, false);
        if (query_context_ptr->getSettingsRef().calculate_text_stack_trace)
            element.stack_trace = getExceptionStackTraceString(vinfo.exception);
    }

    views_log->add(element);
}

void CurrentThread::initializeQuery()
{
    if (unlikely(!current_thread))
        return;
    current_thread->initializeQuery();
    current_thread->deleter = CurrentThread::defaultThreadDeleter;
}

void CurrentThread::attachTo(const ThreadGroupStatusPtr & thread_group)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachQuery(thread_group, true);
    current_thread->deleter = CurrentThread::defaultThreadDeleter;
}

void CurrentThread::attachToIfDetached(const ThreadGroupStatusPtr & thread_group)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachQuery(thread_group, false);
    current_thread->deleter = CurrentThread::defaultThreadDeleter;
}

void CurrentThread::attachQueryContext(ContextPtr query_context)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachQueryContext(query_context);
}

void CurrentThread::finalizePerformanceCounters()
{
    if (unlikely(!current_thread))
        return;
    current_thread->finalizePerformanceCounters();
}

void CurrentThread::detachQuery()
{
    if (unlikely(!current_thread))
        return;
    current_thread->detachQuery(false);
}

void CurrentThread::detachQueryIfNotDetached()
{
    if (unlikely(!current_thread))
        return;
    current_thread->detachQuery(true);
}


CurrentThread::QueryScope::QueryScope(ContextMutablePtr query_context)
{
    CurrentThread::initializeQuery();
    CurrentThread::attachQueryContext(query_context);
    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();
}

CurrentThread::QueryScope::QueryScope(ContextPtr query_context)
{
    if (!query_context->hasQueryContext())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot initialize query scope without query context");

    CurrentThread::initializeQuery();
    CurrentThread::attachQueryContext(query_context);
}

void CurrentThread::QueryScope::logPeakMemoryUsage()
{
    auto group = CurrentThread::getGroup();
    if (!group)
        return;

    log_peak_memory_usage_in_destructor = false;
    group->memory_tracker.logPeakMemoryUsage();
}

CurrentThread::QueryScope::~QueryScope()
{
    try
    {
        if (log_peak_memory_usage_in_destructor)
            logPeakMemoryUsage();

        CurrentThread::detachQueryIfNotDetached();
    }
    catch (...)
    {
        tryLogCurrentException("CurrentThread", __PRETTY_FUNCTION__);
    }
}

}
