#include <Common/ThreadStatus.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/QueryProfiler.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/TraceCollector.h>
#include <common/errnoToString.h>

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
    const Settings & settings = query_context->getSettingsRef();

    query_id = query_context->getCurrentQueryId();
    initQueryProfiler();

    untracked_memory_limit = settings.max_untracked_memory;
    if (settings.memory_profiler_step && settings.memory_profiler_step < UInt64(untracked_memory_limit))
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


void ThreadStatus::attachQueryContext(Context & query_context_)
{
    query_context = &query_context_;

    if (!global_context)
        global_context = &query_context->getGlobalContext();

    if (thread_group)
    {
        std::lock_guard lock(thread_group->mutex);

        thread_group->query_context = query_context;
        if (!thread_group->global_context)
            thread_group->global_context = global_context;
    }

    // Generate new span for thread manually here, because we can't depend
    // on OpenTelemetrySpanHolder due to link order issues.
    // FIXME why and how is this different from setupState()?
    thread_trace_context = query_context->query_trace_context;
    if (thread_trace_context.trace_id)
    {
        thread_trace_context.span_id = thread_local_rng();
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

        logs_queue_ptr = thread_group->logs_queue_ptr;
        fatal_error_callback = thread_group->fatal_error_callback;
        query_context = thread_group->query_context;

        if (!global_context)
            global_context = thread_group->global_context;
    }

    if (query_context)
    {
        applyQuerySettings();

        // Generate new span for thread manually here, because we can't depend
        // on OpenTelemetrySpanHolder due to link order issues.
        thread_trace_context = query_context->query_trace_context;
        if (thread_trace_context.trace_id)
        {
            thread_trace_context.span_id = thread_local_rng();
        }
    }
    else
    {
        thread_trace_context.trace_id = 0;
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

    if (query_context)
    {
        const Settings & settings = query_context->getSettingsRef();
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
    if (query_context)
        close_perf_descriptors = !query_context->getSettingsRef().metrics_perf_events_enabled;

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
        if (global_context && query_context)
        {
            const auto & settings = query_context->getSettingsRef();
            if (settings.log_queries && settings.log_query_threads)
            {
                const auto now = std::chrono::system_clock::now();
                Int64 query_duration_ms = (time_in_microseconds(now) - query_start_time_microseconds) / 1000;
                if (query_duration_ms >= settings.log_queries_min_query_duration_ms.totalMilliseconds())
                {
                    if (auto thread_log = global_context->getQueryThreadLog())
                        logToQueryThreadLog(*thread_log, query_context->getCurrentDatabase(), now);
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::initQueryProfiler()
{
    /// query profilers are useless without trace collector
    if (!global_context || !global_context->hasTraceCollector())
        return;

    const auto & settings = query_context->getSettingsRef();

    try
    {
        if (settings.query_profiler_real_time_period_ns > 0)
            query_profiler_real = std::make_unique<QueryProfilerReal>(thread_id,
                /* period */ static_cast<UInt32>(settings.query_profiler_real_time_period_ns));

        if (settings.query_profiler_cpu_time_period_ns > 0)
            query_profiler_cpu = std::make_unique<QueryProfilerCpu>(thread_id,
                /* period */ static_cast<UInt32>(settings.query_profiler_cpu_time_period_ns));
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
    if (exit_if_already_detached && thread_state == ThreadState::DetachedFromQuery)
    {
        thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
        return;
    }

    assertState({ThreadState::AttachedToQuery}, __PRETTY_FUNCTION__);

    std::shared_ptr<OpenTelemetrySpanLog> opentelemetry_span_log;
    if (thread_trace_context.trace_id && query_context)
    {
        opentelemetry_span_log = query_context->getOpenTelemetrySpanLog();
    }

    if (opentelemetry_span_log)
    {
        // Log the current thread span.
        // We do this manually, because we can't use OpenTelemetrySpanHolder as a
        // ThreadStatus member, because of linking issues. This file is linked
        // separately, so we can reference OpenTelemetrySpanLog here, but if we had
        // the span holder as a field, we would have to reference it in the
        // destructor, which is in another library.
        OpenTelemetrySpanLogElement span;

        span.trace_id = thread_trace_context.trace_id;
        // All child span holders should be finished by the time we detach this
        // thread, so the current span id should be the thread span id. If not,
        // an assertion for a proper parent span in ~OpenTelemetrySpanHolder()
        // is going to fail, because we're going to reset it to zero later in
        // this function.
        span.span_id = thread_trace_context.span_id;
        span.parent_span_id = query_context->query_trace_context.span_id;
        span.operation_name = getThreadName();
        span.start_time_us = query_start_time_microseconds;
        span.finish_time_us =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        span.attribute_names.push_back("clickhouse.thread_id");
        span.attribute_values.push_back(thread_id);

        opentelemetry_span_log->add(span);
    }

    finalizeQueryProfiler();
    finalizePerformanceCounters();

    /// Detach from thread group
    performance_counters.setParent(&ProfileEvents::global_counters);
    memory_tracker.reset();

    /// Must reset pointer to thread_group's memory_tracker, because it will be destroyed two lines below (will reset to its parent).
    memory_tracker.setParent(thread_group->memory_tracker.getParent());

    query_id.clear();
    query_context = nullptr;
    thread_trace_context.trace_id = 0;
    thread_trace_context.span_id = 0;
    thread_group.reset();

    thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;

#if defined(__linux__)
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

    /// TODO: Use written_rows and written_bytes when run time progress is implemented
    elem.written_rows = progress_out.read_rows.load(std::memory_order_relaxed);
    elem.written_bytes = progress_out.read_bytes.load(std::memory_order_relaxed);
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
        }
    }

    if (query_context)
    {
        elem.client_info = query_context->getClientInfo();

        if (query_context->getSettingsRef().log_profile_events != 0)
        {
            /// NOTE: Here we are in the same thread, so we can make memcpy()
            elem.profile_counters = std::make_shared<ProfileEvents::Counters>(performance_counters.getPartiallyAtomicSnapshot());
        }
    }

    thread_log.add(elem);
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

void CurrentThread::attachQueryContext(Context & query_context)
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


CurrentThread::QueryScope::QueryScope(Context & query_context)
{
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
