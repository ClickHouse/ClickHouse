#include "ThreadStatus.h"
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <Interpreters/ProcessList.h>


namespace ProfileEvents
{
    extern const Event RealTimeMicroseconds;
    extern const Event RusageUserTimeMicroseconds;
    extern const Event RusageSystemTimeMicroseconds;
    extern const Event RusagePageReclaims;
    extern const Event RusagePageVoluntaryContextSwitches;
    extern const Event RusagePageInvoluntaryContextSwitches;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


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
    RusageCounters(const struct rusage & rusage, UInt64 real_time_)
    {
        set(rusage, real_time_);
    }

    static RusageCounters zeros(UInt64 real_time)
    {
        RusageCounters res;
        res.real_time = real_time;
        return res;
    }

    static RusageCounters current()
    {
        RusageCounters res;
        res.setFromCurrent();
        return res;
    }

    void setFromCurrent()
    {
        struct rusage rusage;
        getrusage(RUSAGE_THREAD, &rusage);
        set(rusage, getCurrentTimeMicroseconds());
    }

    void set(const struct rusage & rusage, UInt64 real_time_)
    {
        real_time = real_time_;
        user_time = rusage.ru_utime.tv_sec * 1000000UL + rusage.ru_utime.tv_usec;
        sys_time = rusage.ru_utime.tv_sec * 1000000UL + rusage.ru_utime.tv_usec;

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
        RusageCounters current = RusageCounters::current();
        RusageCounters::incrementProfileEvents(current, last_counters);
        last_counters = current;
    }
};


struct ThreadStatus::Payload
{
    RusageCounters last_rusage;
};


//static void QueryThreadStatusOnThreadFinish(void * arg)
//{
//    auto thread_status = static_cast<ThreadStatus *>(arg);
//    thread_status->is_finished = true;
//    LOG_DEBUG(thread_status->log, "Thread " << thread_status->poco_thread_number << " is finished");
//}


ThreadStatus::ThreadStatus()
    : poco_thread_number(Poco::ThreadNumber::get()),
      performance_counters(ProfileEvents::Level::Thread),
      log(&Poco::Logger::get("ThreadStatus"))
{
    LOG_DEBUG(log, "Thread " << poco_thread_number << " created");
}

ThreadStatus::~ThreadStatus()
{
    LOG_DEBUG(log, "Thread " << poco_thread_number << " destroyed");
}

void ThreadStatus::init(QueryStatus * parent_query_, ProfileEvents::Counters * parent_counters, MemoryTracker * parent_memory_tracker)
{
    if (!initialized)
    {
        if (auto counters_parent = performance_counters.parent)
            if (counters_parent != parent_counters)
                LOG_WARNING(current_thread->log, "Parent performance counters are already set, overwrite");

        if (auto tracker_parent = memory_tracker.getParent())
            if (tracker_parent != parent_memory_tracker)
                LOG_WARNING(current_thread->log, "Parent memory tracker is already set, overwrite");

        return;
    }

    initialized = true;
    parent_query = parent_query_;
    performance_counters.parent = parent_counters;
    memory_tracker.setParent(parent_memory_tracker);
    memory_tracker.setDescription("(for thread)");

    onStart();
}

void ThreadStatus::onStart()
{
    payload = std::make_shared<Payload>();

    /// First init of thread rusage counters, set real time to zero, other metrics remain as is
    payload->last_rusage.setFromCurrent();
    RusageCounters::incrementProfileEvents(payload->last_rusage, RusageCounters::zeros(payload->last_rusage.real_time));
}

void ThreadStatus::onExit()
{
    if (!initialized || !payload)
        return;

    RusageCounters::updateProfileEvents(payload->last_rusage);
}

void ThreadStatus::reset()
{
    parent_query = nullptr;
    performance_counters.reset();
    memory_tracker.reset();
    memory_tracker.setParent(nullptr);
    initialized = false;
}


void ThreadStatus::setCurrentThreadParentQuery(QueryStatus * parent_process)
{
    if (!current_thread)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (!parent_process)
    {
        current_thread->init(parent_process, nullptr, nullptr);
        return;
    }

    current_thread->init(parent_process, &parent_process->performance_counters, &parent_process->memory_tracker);

    {
        std::lock_guard lock(parent_process->threads_mutex);
        auto res = parent_process->thread_statuses.emplace(current_thread->poco_thread_number, current_thread);

        if (!res.second && res.first->second.get() != current_thread.get())
            throw Exception("Thread " + std::to_string(current_thread->poco_thread_number) + " is set twice", ErrorCodes::LOGICAL_ERROR);
    }
}

void ThreadStatus::setCurrentThreadFromSibling(const ThreadStatusPtr & sibling_thread)
{
    if (!current_thread)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (sibling_thread == nullptr)
        throw Exception("Sibling thread was not initialized", ErrorCodes::LOGICAL_ERROR);

    current_thread->init(sibling_thread->parent_query, sibling_thread->performance_counters.parent, sibling_thread->memory_tracker.getParent());
}


thread_local ThreadStatusPtr current_thread = ThreadStatus::create();


}
