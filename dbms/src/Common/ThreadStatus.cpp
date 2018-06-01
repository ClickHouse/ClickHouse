#include "ThreadStatus.h"
#include <Poco/Ext/ThreadNumber.h>
#include <common/logger_useful.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Common/CurrentThread.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/ProcessList.h>


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

    CurrentThreadScope()
    {
        try
        {
            {
                std::lock_guard lock(current_thread->mutex);
                current_thread->is_active_thread = true;
            }
            LOG_DEBUG(current_thread->log, "Thread " << current_thread->poco_thread_number << " is started");
        }
        catch (...)
        {
            std::terminate();
        }
    }

    ~CurrentThreadScope()
    {
        try
        {
            CurrentThread::detachQuery();
            LOG_DEBUG(current_thread->log, "Thread " << current_thread->poco_thread_number << " is exited");
            {
                std::lock_guard lock(current_thread->mutex);
                current_thread->is_active_thread = false;
            }
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
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000UL;
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

struct ThreadStatus::Impl
{
    RusageCounters last_rusage;
    TasksStatsCounters last_taskstats;
    TaskStatsInfoGetter info_getter;
};


TasksStatsCounters TasksStatsCounters::current()
{
    TasksStatsCounters res;
    current_thread->impl->info_getter.getStat(res.stat, current_thread->os_thread_id);
    return res;
}



ThreadStatus::ThreadStatus()
    : poco_thread_number(Poco::ThreadNumber::get()),
      performance_counters(ProfileEvents::Level::Thread),
      os_thread_id(TaskStatsInfoGetter::getCurrentTID()),
      log(&Poco::Logger::get("ThreadStatus"))
{
    impl = std::make_unique<Impl>();

    LOG_DEBUG(log, "Thread " << poco_thread_number << " created");
}

ThreadStatusPtr ThreadStatus::create()
{
    return ThreadStatusPtr(new ThreadStatus);
}

ThreadStatus::~ThreadStatus()
{
    LOG_DEBUG(log, "Thread " << poco_thread_number << " destroyed in " << Poco::ThreadNumber::get());
}

void ThreadStatus::attachQuery(
        QueryStatus * parent_query_,
        ProfileEvents::Counters * parent_counters,
        MemoryTracker * parent_memory_tracker,
        bool check_detached)
{
    std::lock_guard lock(mutex);

    if (is_active_query)
    {
        if (check_detached)
            throw Exception("Can't attach query to the thread, it is already attached", ErrorCodes::LOGICAL_ERROR);
        return;
    }

    parent_query = parent_query_;
    performance_counters.setParent(parent_counters);
    memory_tracker.setParent(parent_memory_tracker);
    memory_tracker.setDescription("(for thread)");

    /// Try extract as many information as possible from ProcessList
    if (auto query = getParentQuery())
    {
        /// Attach current thread to list of query threads
        {
            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ":" << __LINE__ << " " << query);
            std::unique_lock lock(query->threads_mutex);

            if (query->thread_statuses.empty())
                query->master_thread = shared_from_this();

            if (!query->thread_statuses.emplace(poco_thread_number, shared_from_this()).second)
                throw Exception("Thread " + std::to_string(poco_thread_number) + " is attached twice", ErrorCodes::LOGICAL_ERROR);

            LOG_DEBUG(log, __PRETTY_FUNCTION__ << ":" << __LINE__ << " " << query);
        }

        query_context = query->tryGetQueryContext();

        if (auto current_query_context = getQueryContext())
        {
            log_to_query_thread_log = current_query_context->getSettingsRef().log_query_threads.value != 0;
            log_profile_events = current_query_context->getSettingsRef().log_profile_events.value != 0;

            if (!getGlobalContext())
                global_context = &current_query_context->getGlobalContext();
        }
    }

    query_start_time_nanoseconds = getCurrentTimeNanoseconds();
    query_start_time = time(nullptr);

    /// First init of thread rusage counters, set real time to zero, other metrics remain as is
    if (is_first_query_of_the_thread)
    {
        impl->last_rusage = RusageCounters::zeros(query_start_time_nanoseconds);
        impl->last_taskstats = TasksStatsCounters::zeros();
        updatePerfomanceCountersImpl();
    }
    else
    {
        impl->last_rusage = RusageCounters::current(query_start_time_nanoseconds);
        impl->last_taskstats = TasksStatsCounters::current();
    }

    is_active_query = true;
}

void ThreadStatus::updatePerfomanceCountersImpl()
{
    try
    {
        RusageCounters::updateProfileEvents(impl->last_rusage, performance_counters);
        TasksStatsCounters::updateProfileEvents(impl->last_taskstats, performance_counters);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::detachQuery(bool thread_exits)
{
    if (!is_active_query)
        return;

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
        performance_counters.setParent(nullptr);
        memory_tracker.setParent(nullptr);
        query_context = nullptr;

        is_active_query = false;
        is_first_query_of_the_thread = false;
        is_active_thread = !thread_exits;
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
    elem.memory_usage = std::max(0, memory_tracker.getPeak());

    elem.thread_name = getThreadName();
    elem.thread_number = poco_thread_number;
    elem.os_thread_id = os_thread_id;

    if (auto query = getParentQuery())
    {
        if (query->master_thread)
        {
            elem.master_thread_number = query->master_thread->poco_thread_number;
            elem.master_os_thread_id = query->master_thread->os_thread_id;
        }

        elem.query = query->query;
        elem.client_info = query->getClientInfo();
    }

    if (log_profile_events)
    {
        /// NOTE: Here we are in the same thread, so we can make memcpy()
        elem.profile_counters = std::make_shared<ProfileEvents::Counters>();
        performance_counters.getPartiallyAtomicSnapshot(*elem.profile_counters);
    }

    thread_log.add(elem);
}

void ThreadStatus::reset()
{
    std::lock_guard lock(mutex);

    parent_query = nullptr;

    if (is_active_query)
        throw Exception("Query is still active", ErrorCodes::LOGICAL_ERROR);
}

}
