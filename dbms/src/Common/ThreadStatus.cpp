#include "ThreadStatus.h"
#include <Poco/Ext/ThreadNumber.h>
#include <common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <Common/TaskStatsInfoGetter.h>
#include <Common/CurrentThread.h>


#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <linux/taskstats.h>


namespace ProfileEvents
{
    extern const Event RealTimeMicroseconds;
    extern const Event RusageUserTimeMicroseconds;
    extern const Event RusageSystemTimeMicroseconds;
    extern const Event RusagePageReclaims;
    extern const Event RusagePageVoluntaryContextSwitches;
    extern const Event RusagePageInvoluntaryContextSwitches;

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


class CurrentThreadScope
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
static thread_local CurrentThreadScope current_thread_scope;



static UInt64 getCurrentTimeMicroseconds(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts;
    clock_gettime(clock_type, &ts);
    return ts.tv_sec * 1000000ULL + ts.tv_nsec / 1000UL;
}

struct RusageCounters
{
    /// In microseconds
    UInt64 real_time = 0;
    UInt64 user_time = 0;
    UInt64 sys_time = 0;

    UInt64 page_reclaims = 0;
    UInt64 voluntary_context_switches = 0;
    UInt64 involuntary_context_switches = 0;

    RusageCounters() = default;
    RusageCounters(const ::rusage & rusage, UInt64 real_time_)
    {
        set(rusage, real_time_);
    }

    static RusageCounters zeros(UInt64 real_time = getCurrentTimeMicroseconds())
    {
        RusageCounters res;
        res.real_time = real_time;
        return res;
    }

    static RusageCounters current()
    {
        RusageCounters res;
        ::rusage rusage;
        ::getrusage(RUSAGE_THREAD, &rusage);
        res.set(rusage, getCurrentTimeMicroseconds());
        return res;
    }

    void set(const ::rusage & rusage, UInt64 real_time_)
    {
        real_time = real_time_;
        user_time = rusage.ru_utime.tv_sec * 1000000UL + rusage.ru_utime.tv_usec / 1000UL;
        sys_time = rusage.ru_stime.tv_sec * 1000000UL + rusage.ru_stime.tv_usec  / 1000UL;

        page_reclaims = static_cast<UInt64>(rusage.ru_minflt);
        voluntary_context_switches = static_cast<UInt64>(rusage.ru_nvcsw);
        involuntary_context_switches = static_cast<UInt64>(rusage.ru_nivcsw);
    }

    static void incrementProfileEvents(const RusageCounters & cur, const RusageCounters & prev)
    {
        ProfileEvents::increment(ProfileEvents::RealTimeMicroseconds, cur.real_time - prev.real_time);
        ProfileEvents::increment(ProfileEvents::RusageUserTimeMicroseconds, cur.user_time - prev.user_time);
        ProfileEvents::increment(ProfileEvents::RusageSystemTimeMicroseconds, cur.sys_time - prev.sys_time);
        ProfileEvents::increment(ProfileEvents::RusagePageReclaims, cur.page_reclaims - prev.page_reclaims);
        ProfileEvents::increment(ProfileEvents::RusagePageVoluntaryContextSwitches, cur.voluntary_context_switches - prev.voluntary_context_switches);
        ProfileEvents::increment(ProfileEvents::RusagePageInvoluntaryContextSwitches, cur.involuntary_context_switches - prev.involuntary_context_switches);
    }

    static void updateProfileEvents(RusageCounters & last_counters)
    {
        auto current_counters = current();
        incrementProfileEvents(current_counters, last_counters);
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

    static void incrementProfileEvents(const TasksStatsCounters & curr, const TasksStatsCounters & prev)
    {
        ProfileEvents::increment(ProfileEvents::OSReadBytes,  curr.stat.read_bytes  - prev.stat.read_bytes);
        ProfileEvents::increment(ProfileEvents::OSWriteBytes, curr.stat.write_bytes - prev.stat.write_bytes);
        ProfileEvents::increment(ProfileEvents::OSReadChars,  curr.stat.read_char   - prev.stat.read_char);
        ProfileEvents::increment(ProfileEvents::OSWriteChars, curr.stat.write_char  - prev.stat.write_char);
    }

    static void updateProfileEvents(TasksStatsCounters & last_counters)
    {
        auto current_counters = current();
        incrementProfileEvents(current_counters, last_counters);
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
        QueryStatus *parent_query_,
        ProfileEvents::Counters *parent_counters,
        MemoryTracker *parent_memory_tracker,
        bool check_detached)
{
    std::lock_guard lock(mutex);

    if (check_detached && is_active_query)
        throw Exception("Query is already active", ErrorCodes::LOGICAL_ERROR);

    if (auto counters_parent = performance_counters.parent)
        if (counters_parent != parent_counters)
            LOG_WARNING(log, "Parent performance counters are already set, overwrite");

    if (auto tracker_parent = memory_tracker.getParent())
        if (tracker_parent != parent_memory_tracker)
            LOG_WARNING(log, "Parent memory tracker is already set, overwrite");

    parent_query = parent_query_;
    performance_counters.parent = parent_counters;
    memory_tracker.setParent(parent_memory_tracker);
    memory_tracker.setDescription("(for thread)");

    /// Attach current thread to list of query threads
    if (parent_query)
    {
        std::lock_guard lock(parent_query->threads_mutex);
        auto res = parent_query->thread_statuses.emplace(current_thread->poco_thread_number, current_thread);

        if (!res.second && res.first->second.get() != current_thread.get())
            throw Exception("Thread " + std::to_string(current_thread->poco_thread_number) + " is set twice", ErrorCodes::LOGICAL_ERROR);
    }

    /// First init of thread rusage counters, set real time to zero, other metrics remain as is
    if (is_first_query_of_the_thread)
    {
        impl->last_rusage = RusageCounters::zeros();
        impl->last_taskstats = TasksStatsCounters::zeros();
        updatePerfomanceCountersImpl();
    }
    else
    {
        impl->last_rusage = RusageCounters::current();
        impl->last_taskstats = TasksStatsCounters::current();
    }

    is_active_query = true;
}

void ThreadStatus::updatePerfomanceCountersImpl()
{
    try
    {
        RusageCounters::incrementProfileEvents(RusageCounters::current(), impl->last_rusage);
        TasksStatsCounters::incrementProfileEvents(TasksStatsCounters::current(), impl->last_taskstats);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::detachQuery()
{
    std::lock_guard lock(mutex);
    if (!is_active_query)
        return;

    updatePerfomanceCountersImpl();

    is_first_query_of_the_thread = false;
    is_active_query = false;

    /// Detach from parent
    performance_counters.setParent(nullptr);
    memory_tracker.setParent(nullptr);
}

void ThreadStatus::reset()
{
    std::lock_guard lock(mutex);

    if (is_active_query)
        throw Exception("Query is still active", ErrorCodes::LOGICAL_ERROR);

    performance_counters.reset();
    memory_tracker.reset();

    performance_counters.setParent(nullptr);
    memory_tracker.setParent(nullptr);
}

}
