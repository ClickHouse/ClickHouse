#include "ThreadStatus.h"
#include <common/logger_useful.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/ProcessList.h>

#include <Poco/Thread.h>
#include <Poco/Ext/ThreadNumber.h>

#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <linux/taskstats.h>


namespace ProfileEvents
{
    extern const Event RealTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
    extern const Event SoftPageFaults;
    extern const Event HardPageFaults;
    extern const Event VoluntaryContextSwitches;
    extern const Event InvoluntaryContextSwitches;

    extern const Event OSIOWaitMicroseconds;
    extern const Event OSCPUWaitMicroseconds;
    extern const Event OSCPUVirtualTimeMicroseconds;
    extern const Event OSReadChars;
    extern const Event OSWriteChars;
    extern const Event OSReadBytes;
    extern const Event OSWriteBytes;
}


namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int PTHREAD_ERROR;
}


/// Implicitly finalizes current thread in the destructor
class ThreadStatus::CurrentThreadScope
{
public:

    CurrentThreadScope() = default;

    ~CurrentThreadScope()
    {
        try
        {
            ThreadStatus & thread = *CurrentThread::get();

            LOG_TRACE(thread.log, "Thread " << thread.thread_number << " exited");
            thread.detachQuery(true, true);
        }
        catch (...)
        {
            std::terminate();
        }
    }
};


thread_local ThreadStatusPtr current_thread = ThreadStatus::create();

/// Order of current_thread and current_thread_scope matters
static thread_local ThreadStatus::CurrentThreadScope current_thread_scope;


/// Handles overflow
template <typename TUInt>
inline TUInt safeDiff(TUInt prev, TUInt curr)
{
    return curr >= prev ? curr - prev : 0;
}


static UInt64 getCurrentTimeNanoseconds(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts;
    clock_gettime(clock_type, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}


struct RusageCounters
{
    /// In nanoseconds
    UInt64 real_time = 0;
    UInt64 user_time = 0;
    UInt64 sys_time = 0;

    UInt64 soft_page_faults = 0;
    UInt64 hard_page_faults = 0;

    RusageCounters() = default;
    RusageCounters(const ::rusage & rusage_, UInt64 real_time_)
    {
        set(rusage_, real_time_);
    }

    void set(const ::rusage & rusage, UInt64 real_time_)
    {
        real_time = real_time_;
        user_time = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec;
        sys_time = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec;

        soft_page_faults = static_cast<UInt64>(rusage.ru_minflt);
        hard_page_faults = static_cast<UInt64>(rusage.ru_majflt);
    }

    static RusageCounters zeros(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        RusageCounters res;
        res.real_time = real_time_;
        return res;
    }

    static RusageCounters current(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        ::rusage rusage;
        ::getrusage(RUSAGE_THREAD, &rusage);
        return RusageCounters(rusage, real_time_);
    }

    static void incrementProfileEvents(const RusageCounters & prev, const RusageCounters & curr, ProfileEvents::Counters & profile_events)
    {
        profile_events.increment(ProfileEvents::RealTimeMicroseconds,   (curr.real_time - prev.real_time) / 1000U);
        profile_events.increment(ProfileEvents::UserTimeMicroseconds,   (curr.user_time - prev.user_time) / 1000U);
        profile_events.increment(ProfileEvents::SystemTimeMicroseconds, (curr.sys_time - prev.sys_time) / 1000U);

        profile_events.increment(ProfileEvents::SoftPageFaults, curr.soft_page_faults - prev.soft_page_faults);
        profile_events.increment(ProfileEvents::HardPageFaults, curr.hard_page_faults - prev.hard_page_faults);
    }

    static void updateProfileEvents(RusageCounters & last_counters, ProfileEvents::Counters & profile_events)
    {
        auto current_counters = current();
        incrementProfileEvents(last_counters, current_counters, profile_events);
        last_counters = current_counters;
    }
};

struct TasksStatsCounters
{
    ::taskstats stat;

    TasksStatsCounters() = default;

    static TasksStatsCounters zeros()
    {
        TasksStatsCounters res;
        memset(&res.stat, 0, sizeof(stat));
        return res;
    }

    static TasksStatsCounters current();

    static void incrementProfileEvents(const TasksStatsCounters & prev, const TasksStatsCounters & curr, ProfileEvents::Counters & profile_events)
    {
        profile_events.increment(ProfileEvents::OSCPUWaitMicroseconds,
                                 safeDiff(prev.stat.cpu_delay_total, curr.stat.cpu_delay_total) / 1000U);
        profile_events.increment(ProfileEvents::OSIOWaitMicroseconds,
                                 safeDiff(prev.stat.blkio_delay_total, curr.stat.blkio_delay_total) / 1000U);
        profile_events.increment(ProfileEvents::OSCPUVirtualTimeMicroseconds,
                                 safeDiff(prev.stat.cpu_run_virtual_total, curr.stat.cpu_run_virtual_total) / 1000U);

        /// Too old struct version, do not read new fields
        if (curr.stat.version < TASKSTATS_VERSION)
            return;

        profile_events.increment(ProfileEvents::OSReadChars,  safeDiff(prev.stat.read_char, curr.stat.read_char));
        profile_events.increment(ProfileEvents::OSWriteChars, safeDiff(prev.stat.write_char, curr.stat.write_char));
        profile_events.increment(ProfileEvents::OSReadBytes,  safeDiff(prev.stat.read_bytes, curr.stat.read_bytes));
        profile_events.increment(ProfileEvents::OSWriteBytes, safeDiff(prev.stat.write_bytes, curr.stat.write_bytes));
    }

    static void updateProfileEvents(TasksStatsCounters & last_counters, ProfileEvents::Counters & profile_events)
    {
        auto current_counters = current();
        incrementProfileEvents(last_counters, current_counters, profile_events);
        last_counters = current_counters;
    }
};

TasksStatsCounters TasksStatsCounters::current()
{
    TasksStatsCounters res;
    current_thread->taskstats_getter->getStat(res.stat, current_thread->os_thread_id);
    return res;
}


ThreadStatus::ThreadStatus()
{
    thread_number = Poco::ThreadNumber::get();
    os_thread_id = TaskStatsInfoGetter::getCurrentTID();

    last_rusage = std::make_unique<RusageCounters>();
    last_taskstats = std::make_unique<TasksStatsCounters>();
    taskstats_getter = std::make_unique<TaskStatsInfoGetter>();

    memory_tracker.setDescription("(for thread)");
    log = &Poco::Logger::get("ThreadStatus");

    /// NOTE: It is important not to do any non-trivial actions (like updating ProfileEvents or logging) before ThreadStatus is created
    /// Otherwise it could lead to SIGSEGV due to current_thread dereferencing
}

ThreadStatusPtr ThreadStatus::create()
{
    return ThreadStatusPtr(new ThreadStatus);
}

ThreadStatus::~ThreadStatus() = default;

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

    *last_rusage = RusageCounters::current(query_start_time_nanoseconds);
    has_permissions_for_taskstats = TaskStatsInfoGetter::checkProcessHasRequiredPermissions();
    if (has_permissions_for_taskstats)
        *last_taskstats = TasksStatsCounters::current();
}

void ThreadStatus::updatePerformanceCounters()
{
    try
    {
        RusageCounters::updateProfileEvents(*last_rusage, performance_counters);
        if (has_permissions_for_taskstats)
            TasksStatsCounters::updateProfileEvents(*last_taskstats, performance_counters);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
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


void ThreadStatus::assertState(const std::initializer_list<int> & permitted_states, const char * description)
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

void ThreadStatus::attachInternalTextLogsQueue(const InternalTextLogsQueuePtr & logs_queue)
{
    logs_queue_ptr = logs_queue;

    if (!thread_group)
        return;

    std::unique_lock lock(thread_group->mutex);
    thread_group->logs_queue_ptr = logs_queue;
}

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

}
