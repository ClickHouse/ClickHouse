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
    extern const Event OSReadBytes;
    extern const Event OSWriteBytes;
    extern const Event OSReadChars;
    extern const Event OSWriteChars;
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

            LOG_DEBUG(thread.log, "Thread " << thread.thread_number << " exited");
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
    UInt64 voluntary_context_switches = 0;
    UInt64 involuntary_context_switches = 0;

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
        voluntary_context_switches = static_cast<UInt64>(rusage.ru_nvcsw);
        involuntary_context_switches = static_cast<UInt64>(rusage.ru_nivcsw);
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
        profile_events.increment(ProfileEvents::OSIOWaitMicroseconds,
                                 safeDiff(prev.stat.blkio_delay_total, curr.stat.blkio_delay_total) / 1000U);
        profile_events.increment(ProfileEvents::OSReadBytes,  safeDiff(prev.stat.read_bytes, curr.stat.read_bytes));
        profile_events.increment(ProfileEvents::OSWriteBytes, safeDiff(prev.stat.write_bytes, curr.stat.write_bytes));
        profile_events.increment(ProfileEvents::OSReadChars,  safeDiff(prev.stat.read_char, curr.stat.read_char));
        profile_events.increment(ProfileEvents::OSWriteChars, safeDiff(prev.stat.write_char, curr.stat.write_char));
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
    if (thread_state != ThreadState::QueryInitializing && thread_state != ThreadState::DetachedFromQuery)
        throw Exception("Unexpected thread state " + std::to_string(getCurrentState()) + __PRETTY_FUNCTION__, ErrorCodes::LOGICAL_ERROR);

    thread_state = ThreadState::QueryInitializing;
}

void ThreadStatus::attachQuery(
        QueryStatus * parent_query_,
        ProfileEvents::Counters * parent_counters,
        MemoryTracker * parent_memory_tracker,
        const SystemLogsQueueWeakPtr & logs_queue_ptr_,
        bool check_detached)
{
    if (thread_state == ThreadState::AttachedToQuery)
    {
        if (check_detached)
            throw Exception("Can't attach query to the thread, it is already attached", ErrorCodes::LOGICAL_ERROR);
        return;
    }

    if (thread_state != ThreadState::DetachedFromQuery && thread_state != ThreadState::QueryInitializing)
        throw Exception("Unexpected thread state " + std::to_string(getCurrentState()) + __PRETTY_FUNCTION__, ErrorCodes::LOGICAL_ERROR);

    {
        std::lock_guard lock(mutex);
        parent_query = parent_query_;
        performance_counters.setParent(parent_counters);
        memory_tracker.setParent(parent_memory_tracker);
        logs_queue_ptr = logs_queue_ptr_;
    }

    /// Clear stats from previous query if a new query is started
    /// TODO: make separate query_thread_performance_counters and thread_performance_counters
    performance_counters.resetCounters();
    memory_tracker.resetCounters();
    memory_tracker.setDescription("(for thread)");

    /// Try extract as many information as possible from ProcessList
    if (parent_query)
    {
        /// Attach current thread to list of query threads
        {
            std::unique_lock lock(parent_query->threads_mutex);

            if (parent_query->thread_statuses.empty())
                parent_query->master_thread = shared_from_this();

            if (!parent_query->thread_statuses.emplace(thread_number, shared_from_this()).second)
                throw Exception("Thread " + std::to_string(thread_number) + " is attached twice", ErrorCodes::LOGICAL_ERROR);
        }

        query_context = parent_query->tryGetQueryContext();
        if (query_context)
        {
            log_to_query_thread_log = query_context->getSettingsRef().log_query_threads.value != 0;
            log_profile_events = query_context->getSettingsRef().log_profile_events.value != 0;

            if (!getGlobalContext())
                global_context = &query_context->getGlobalContext();
        }
    }

    thread_state = ThreadState::AttachedToQuery;

    query_start_time_nanoseconds = getCurrentTimeNanoseconds();
    query_start_time = time(nullptr);
    ++queries_started;

    *last_rusage = RusageCounters::current(query_start_time_nanoseconds);
    *last_taskstats = TasksStatsCounters::current();
}

void ThreadStatus::detachQuery(bool exit_if_already_detached, bool thread_exits)
{
    if (exit_if_already_detached && thread_state == ThreadState::DetachedFromQuery)
    {
        thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
        return;
    }

    if (thread_state != ThreadState::AttachedToQuery && thread_state != ThreadState::QueryInitializing)
        throw Exception("Unexpected thread state " + std::to_string(getCurrentState()) + __PRETTY_FUNCTION__, ErrorCodes::LOGICAL_ERROR);

    updatePerfomanceCountersImpl();

    try
    {
        if (log_to_query_thread_log)
            if (auto global_context = getGlobalContext())
                if (auto thread_log = global_context->getQueryThreadLog())
                    logToQueryThreadLog(*thread_log);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }

    {
        std::lock_guard lock(mutex);

        /// Detach from parent
        performance_counters.setParent(&ProfileEvents::global_counters);
        memory_tracker.setParent(nullptr);
        query_context = nullptr;
    }

    thread_state = thread_exits ? ThreadState::Died : ThreadState::DetachedFromQuery;
    log_to_query_thread_log = true;
    log_profile_events = true;
}


void ThreadStatus::updatePerfomanceCountersImpl()
{
    try
    {
        RusageCounters::updateProfileEvents(*last_rusage, performance_counters);
        TasksStatsCounters::updateProfileEvents(*last_taskstats, performance_counters);
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
    elem.memory_usage = memory_tracker.getPeak();

    elem.thread_name = getThreadName();
    elem.thread_number = thread_number;
    elem.os_thread_id = os_thread_id;

    if (parent_query)
    {
        {
            std::shared_lock threads_mutex(parent_query->threads_mutex);

            if (parent_query->master_thread)
            {
                elem.master_thread_number = parent_query->master_thread->thread_number;
                elem.master_os_thread_id = parent_query->master_thread->os_thread_id;
            }
        }

        elem.query = parent_query->query;
        elem.client_info = parent_query->getClientInfo();
    }

    if (log_profile_events)
    {
        /// NOTE: Here we are in the same thread, so we can make memcpy()
        elem.profile_counters = std::make_shared<ProfileEvents::Counters>(performance_counters.getPartiallyAtomicSnapshot());
    }

    thread_log.add(elem);
}

void ThreadStatus::clean()
{
    {
        std::lock_guard lock(mutex);
        parent_query = nullptr;
    }

    if (thread_state != ThreadState::DetachedFromQuery && thread_state != ThreadState::Died)
        throw Exception("Unexpected thread state " + std::to_string(getCurrentState()) + __PRETTY_FUNCTION__, ErrorCodes::LOGICAL_ERROR);
}

}
