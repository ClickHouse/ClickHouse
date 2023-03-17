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
#include <Parsers/queryNormalization.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/QueryProfiler.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/noexcept_scope.h>
#include <Common/DateLUT.h>
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

ThreadGroupStatus::ThreadGroupStatus(ContextPtr query_context_, FatalErrorCallback fatal_error_callback_)
    : master_thread_id(CurrentThread::get().thread_id)
    , query_context(query_context_)
    , global_context(query_context_->getGlobalContext())
    , fatal_error_callback(fatal_error_callback_)
{}

std::vector<UInt64> ThreadGroupStatus::getInvolvedThreadIds() const
{
    std::vector<UInt64> res;

    {
        std::lock_guard lock(mutex);
        res.assign(thread_ids.begin(), thread_ids.end());
    }

    return res;
}

void ThreadGroupStatus::linkThread(UInt64 thread_it)
{
    std::lock_guard lock(mutex);
    thread_ids.insert(thread_it);
}

ThreadGroupStatusPtr ThreadGroupStatus::createForQuery(ContextPtr query_context_, std::function<void()> fatal_error_callback_)
{
    auto group = std::make_shared<ThreadGroupStatus>(query_context_, std::move(fatal_error_callback_));
    group->memory_tracker.setDescription("(for query)");
    return group;
}

void ThreadGroupStatus::attachQueryForLog(const String & query_, UInt64 normalized_hash)
{
    auto hash = normalized_hash ? normalized_hash : normalizedQueryHash<false>(query_);

    std::lock_guard lock(mutex);
    shared_data.query_for_logs = query_;
    shared_data.normalized_query_hash = hash;
}

void ThreadStatus::attachQueryForLog(const String & query_)
{
    local_data.query_for_logs = query_;
    local_data.normalized_query_hash = normalizedQueryHash<false>(query_);

    if (!thread_group)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No thread group attached to the thread {}", thread_id);

    thread_group->attachQueryForLog(local_data.query_for_logs, local_data.normalized_query_hash);
}

void ThreadGroupStatus::attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue)
{
    std::lock_guard lock(mutex);
    shared_data.profile_queue_ptr = profile_queue;
}

void ThreadStatus::attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & profile_queue)
{
    if (!thread_group)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "No thread group attached to the thread {}", thread_id);

    local_data.profile_queue_ptr = profile_queue;
    thread_group->attachInternalProfileEventsQueue(profile_queue);
}

void CurrentThread::attachInternalProfileEventsQueue(const InternalProfileEventsQueuePtr & queue)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachInternalProfileEventsQueue(queue);
}

void CurrentThread::attachQueryForLog(const String & query_)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachQueryForLog(query_);
}

void ThreadStatus::applyQuerySettings()
{
    auto query_context_ptr = query_context.lock();
    if (!query_context_ptr)
        return;

    const Settings & settings = query_context_ptr->getSettingsRef();

    query_id_from_query_context = query_context_ptr->getCurrentQueryId();
    initQueryProfiler();

    untracked_memory_limit = settings.max_untracked_memory;
    if (settings.memory_profiler_step && settings.memory_profiler_step < static_cast<UInt64>(untracked_memory_limit))
        untracked_memory_limit = settings.memory_profiler_step;

#if defined(OS_LINUX)
    /// Set "nice" value if required.
    Int32 new_os_thread_priority = static_cast<Int32>(settings.os_thread_priority);
    if (new_os_thread_priority && hasLinuxCapability(CAP_SYS_NICE))
    {
        LOG_TRACE(log, "Setting nice to {}", new_os_thread_priority);

        if (0 != setpriority(PRIO_PROCESS, static_cast<unsigned>(thread_id), new_os_thread_priority))
            throwFromErrno("Cannot 'setpriority'", ErrorCodes::CANNOT_SET_THREAD_PRIORITY);

        os_thread_priority = new_os_thread_priority;
    }
#endif
}

void ThreadStatus::attachToGroupImpl(const ThreadGroupStatusPtr & thread_group_)
{
    /// Attach or init current thread to thread group and copy useful information from it
    thread_group = thread_group_;
    thread_group->linkThread(thread_id);

    performance_counters.setParent(&thread_group->performance_counters);
    memory_tracker.setParent(&thread_group->memory_tracker);

    query_context = thread_group->query_context;
    global_context = thread_group->global_context;

    fatal_error_callback = thread_group->fatal_error_callback;

    local_data = thread_group->getSharedData();

    applyQuerySettings();
    initPerformanceCounters();
}

void ThreadStatus::detachFromGroup()
{
    if (!thread_group)
        return;

    LockMemoryExceptionInThread lock_memory_tracker(VariableContext::Global);

    /// flash untracked memory before resetting memory_tracker parent
    flushUntrackedMemory();

    finalizeQueryProfiler();
    finalizePerformanceCounters();

    performance_counters.setParent(&ProfileEvents::global_counters);

    memory_tracker.reset();
    memory_tracker.setParent(thread_group->memory_tracker.getParent());

    thread_group.reset();

    query_id_from_query_context.clear();
    query_context.reset();

    local_data = {};

    fatal_error_callback = {};

#if defined(OS_LINUX)
    if (os_thread_priority)
    {
        LOG_TRACE(log, "Resetting nice");

        if (0 != setpriority(PRIO_PROCESS, static_cast<int>(thread_id), 0))
            LOG_ERROR(log, "Cannot 'setpriority' back to zero: {}", errnoToString());

        os_thread_priority = 0;
    }
#endif
}

void ThreadStatus::setInternalThread()
{
    chassert(!query_profiler_real && !query_profiler_cpu);
    internal_thread = true;
}

void ThreadStatus::attachToGroup(const ThreadGroupStatusPtr & thread_group_, bool check_detached)
{
    if (thread_group && check_detached)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't attach query to the thread, it is already attached");

    if (!thread_group_)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Attempt to attach to nullptr thread group");

    if (thread_group)
        return;

    deleter = [this] () { detachFromGroup(); };
    attachToGroupImpl(thread_group_);
}

ProfileEvents::Counters * ThreadStatus::attachProfileCountersScope(ProfileEvents::Counters * performance_counters_scope)
{
    ProfileEvents::Counters * prev_counters = current_performance_counters;

    if (current_performance_counters == performance_counters_scope)
        /// Allow to attach the same scope multiple times
        return prev_counters;

    /// Avoid cycles when exiting local scope and attaching back to current thread counters
    if (performance_counters_scope != &performance_counters)
        performance_counters_scope->setParent(&performance_counters);

    current_performance_counters = performance_counters_scope;

    return prev_counters;
}

void ThreadStatus::TimePoint::setUp()
{
    point = std::chrono::system_clock::now();
}

UInt64 ThreadStatus::TimePoint::nanoseconds() const
{
    return timeInNanoseconds(point);
}

UInt64 ThreadStatus::TimePoint::microseconds() const
{
    return timeInMicroseconds(point);
}

UInt64 ThreadStatus::TimePoint::seconds() const
{
    return timeInSeconds(point);
}

void ThreadStatus::initPerformanceCounters()
{
    performance_counters_finalized = false;

    /// Clear stats from previous query if a new query is started
    /// TODO: make separate query_thread_performance_counters and thread_performance_counters
    performance_counters.resetCounters();
    memory_tracker.resetCounters();
    memory_tracker.setDescription("(for thread)");

    query_start_time.setUp();

    // query_start_time.nanoseconds cannot be used here since RUsageCounters expect CLOCK_MONOTONIC
    *last_rusage = RUsageCounters::current();

    if (!internal_thread)
    {
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
}

void ThreadStatus::finalizePerformanceCounters()
{
    if (performance_counters_finalized || internal_thread)
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
                Int64 query_duration_ms = std::chrono::duration_cast<std::chrono::microseconds>(now - query_start_time.point).count();
                if (query_duration_ms >= settings.log_queries_min_query_duration_ms.totalMilliseconds())
                {
                    if (auto thread_log = global_context_ptr->getQueryThreadLog())
                        logToQueryThreadLog(*thread_log, query_context_ptr->getCurrentDatabase());
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
    if (internal_thread)
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

void ThreadStatus::logToQueryThreadLog(QueryThreadLog & thread_log, const String & current_database)
{
    QueryThreadLogElement elem;

    // construct current_time and current_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    TimePoint current_time;
    current_time.setUp();

    elem.event_time = current_time.seconds();
    elem.event_time_microseconds = current_time.microseconds();
    elem.query_start_time = query_start_time.seconds();
    elem.query_start_time_microseconds = query_start_time.microseconds();
    elem.query_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(current_time.point - query_start_time.point).count();

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
        elem.master_thread_id = thread_group->master_thread_id;
        elem.query = local_data.query_for_logs;
        elem.normalized_query_hash = local_data.normalized_query_hash;
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

    element.event_time = timeInSeconds(vinfo.runtime_stats->event_time);
    element.event_time_microseconds = timeInMicroseconds(vinfo.runtime_stats->event_time);
    element.view_duration_ms = vinfo.runtime_stats->elapsed_ms;

    element.initial_query_id = query_id_from_query_context;
    element.view_name = vinfo.table_id.getFullTableName();
    element.view_uuid = vinfo.table_id.uuid;
    element.view_type = vinfo.runtime_stats->type;
    if (vinfo.query)
        element.view_query = getCleanQueryAst(vinfo.query, query_context_ptr);
    element.view_target = vinfo.runtime_stats->target_name;

    element.read_rows = progress_in.read_rows.load(std::memory_order_relaxed);
    element.read_bytes = progress_in.read_bytes.load(std::memory_order_relaxed);
    element.written_rows = progress_out.written_rows.load(std::memory_order_relaxed);
    element.written_bytes = progress_out.written_bytes.load(std::memory_order_relaxed);
    element.peak_memory_usage = memory_tracker.getPeak() > 0 ? memory_tracker.getPeak() : 0;
    if (query_context_ptr->getSettingsRef().log_profile_events != 0)
        element.profile_counters = std::make_shared<ProfileEvents::Counters::Snapshot>(
                performance_counters.getPartiallyAtomicSnapshot());

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

void CurrentThread::attachToGroup(const ThreadGroupStatusPtr & thread_group)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachToGroup(thread_group, true);
}

void CurrentThread::attachToGroupIfDetached(const ThreadGroupStatusPtr & thread_group)
{
    if (unlikely(!current_thread))
        return;
    current_thread->attachToGroup(thread_group, false);
}

void CurrentThread::finalizePerformanceCounters()
{
    if (unlikely(!current_thread))
        return;
    current_thread->finalizePerformanceCounters();
}

void CurrentThread::detachFromGroupIfNotDetached()
{
    if (unlikely(!current_thread))
        return;
    current_thread->detachFromGroup();
}

CurrentThread::QueryScope::QueryScope(ContextMutablePtr query_context, std::function<void()> fatal_error_callback)
{
    if (!query_context->hasQueryContext())
        query_context->makeQueryContext();

    auto group = ThreadGroupStatus::createForQuery(query_context, std::move(fatal_error_callback));
    CurrentThread::attachToGroup(group);
}

CurrentThread::QueryScope::QueryScope(ContextPtr query_context, std::function<void()> fatal_error_callback)
{
    if (!query_context->hasQueryContext())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot initialize query scope without query context");

    auto group = ThreadGroupStatus::createForQuery(query_context, std::move(fatal_error_callback));
    CurrentThread::attachToGroup(group);
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

        CurrentThread::detachFromGroupIfNotDetached();
    }
    catch (...)
    {
        tryLogCurrentException("CurrentThread", __PRETTY_FUNCTION__);
    }
}

}
