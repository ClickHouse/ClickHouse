#include "ThreadStatus.h"
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>
#include <Interpreters/ProcessList.h>
#include <Common/TaskStatsInfoGetter.h>


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

    static RusageCounters zeros(UInt64 real_time)
    {
        RusageCounters res;
        res.real_time = real_time;
        return res;
    }

    static RusageCounters current()
    {
        RusageCounters res;
        res.setCurrent();
        return res;
    }

    void setCurrent()
    {
        ::rusage rusage;
        ::getrusage(RUSAGE_THREAD, &rusage);
        set(rusage, getCurrentTimeMicroseconds());
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
        RusageCounters current = RusageCounters::current();
        RusageCounters::incrementProfileEvents(current, last_counters);
        last_counters = current;
    }
};


struct ThreadStatus::Impl
{
    RusageCounters last_rusage;
    TaskStatsInfoGetter info_getter;
};


//static void QueryThreadStatusOnThreadFinish(void * arg)
//{
//    auto thread_status = static_cast<ThreadStatus *>(arg);
//    thread_status->is_finished = true;
//    LOG_DEBUG(thread_status->log, "Thread " << thread_status->poco_thread_number << " is finished");
//}

static pthread_once_t once_query_at_exit_callback = PTHREAD_ONCE_INIT;
static pthread_key_t tid_key_at_exit;

static void thread_destructor(void * data)
{
    auto thread_status = static_cast<ThreadStatus *>(data);
    thread_status->onExit();
    LOG_DEBUG(thread_status->log, "Destruct thread " << thread_status->poco_thread_number);
    thread_status->thread_exited = true;
}

static void thread_create_at_exit_key() {
    if (0 != pthread_key_create(&tid_key_at_exit, thread_destructor))
        throw Exception("Failed pthread_key_create", ErrorCodes::PTHREAD_ERROR);
}


ThreadStatus::ThreadStatus()
    : poco_thread_number(Poco::ThreadNumber::get()),
      performance_counters(ProfileEvents::Level::Thread),
      os_thread_id(TaskStatsInfoGetter::getCurrentTID()),
      log(&Poco::Logger::get("ThreadStatus"))
{
    impl = std::make_shared<Impl>();

    LOG_DEBUG(log, "Thread " << poco_thread_number << " created");

    if (0 != pthread_once(&once_query_at_exit_callback, thread_create_at_exit_key))
        throw Exception("Failed pthread_once", ErrorCodes::PTHREAD_ERROR);

    if (nullptr != pthread_getspecific(tid_key_at_exit))
        throw Exception("pthread_getspecific is already set", ErrorCodes::LOGICAL_ERROR);

    if (0 != pthread_setspecific(tid_key_at_exit, static_cast<void *>(this)))
        throw Exception("Failed pthread_setspecific", ErrorCodes::PTHREAD_ERROR);
}

ThreadStatus::~ThreadStatus()
{
    LOG_DEBUG(log, "Thread " << poco_thread_number << " destroyed");
}

void ThreadStatus::init(QueryStatus * parent_query_, ProfileEvents::Counters * parent_counters, MemoryTracker * parent_memory_tracker)
{
    if (initialized)
    {
        if (auto counters_parent = performance_counters.parent)
            if (counters_parent != parent_counters)
                LOG_WARNING(log, "Parent performance counters are already set, overwrite");

        if (auto tracker_parent = memory_tracker.getParent())
            if (tracker_parent != parent_memory_tracker)
                LOG_WARNING(log, "Parent memory tracker is already set, overwrite");

        return;
    }

    parent_query = parent_query_;
    performance_counters.parent = parent_counters;
    memory_tracker.setParent(parent_memory_tracker);
    memory_tracker.setDescription("(for thread)");
    initialized = true;

    /// Attach current thread to list of query threads
    if (parent_query)
    {
        std::lock_guard lock(parent_query->threads_mutex);
        auto res = parent_query->thread_statuses.emplace(current_thread->poco_thread_number, current_thread);

        if (!res.second && res.first->second.get() != current_thread.get())
            throw Exception("Thread " + std::to_string(current_thread->poco_thread_number) + " is set twice", ErrorCodes::LOGICAL_ERROR);
    }

    onStart();
}

void ThreadStatus::onStart()
{
    /// First init of thread rusage counters, set real time to zero, other metrics remain as is
    impl->last_rusage.setCurrent();
    RusageCounters::incrementProfileEvents(impl->last_rusage, RusageCounters::zeros(impl->last_rusage.real_time));
}

void ThreadStatus::onExit()
{
    RusageCounters::updateProfileEvents(impl->last_rusage);

    try
    {
        ::taskstats stat;
        TaskStatsInfoGetter info_getter;
        info_getter.getStat(stat);

        ProfileEvents::increment(ProfileEvents::OSReadBytes, stat.read_bytes);
        ProfileEvents::increment(ProfileEvents::OSWriteBytes, stat.write_bytes);
        ProfileEvents::increment(ProfileEvents::OSReadChars, stat.read_char);
        ProfileEvents::increment(ProfileEvents::OSWriteChars, stat.write_char);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

void ThreadStatus::reset()
{
    std::lock_guard lock(mutex);

    initialized = false;
    parent_query = nullptr;
    performance_counters.reset();
    memory_tracker.reset();
    memory_tracker.setParent(nullptr);
}


void ThreadStatus::setCurrentThreadParentQuery(QueryStatus * parent_process)
{
    if (!current_thread)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (!parent_process)
    {
        current_thread->init(nullptr, nullptr, nullptr);
        return;
    }

    current_thread->init(parent_process, &parent_process->performance_counters, &parent_process->memory_tracker);
}

void ThreadStatus::setCurrentThreadFromSibling(const ThreadStatusPtr & sibling_thread)
{
    if (!current_thread)
        throw Exception("Thread #" + std::to_string(Poco::ThreadNumber::get()) + " status was not initialized", ErrorCodes::LOGICAL_ERROR);

    if (sibling_thread == nullptr)
        throw Exception("Sibling thread was not initialized", ErrorCodes::LOGICAL_ERROR);

    std::lock_guard lock(sibling_thread->mutex);
    current_thread->init(sibling_thread->parent_query, sibling_thread->performance_counters.parent, sibling_thread->memory_tracker.getParent());
}


struct ScopeCurrentThread
{
    ScopeCurrentThread()
    {
        if (!current_thread)
            std::terminate(); // current_thread must be initialized
    }

    ~ScopeCurrentThread()
    {
        if (!current_thread)
            std::terminate(); // current_thread must be initialized

        if (Poco::ThreadNumber::get() != current_thread->poco_thread_number)
            std::terminate(); // unexpected thread number

        current_thread->onExit();
        LOG_DEBUG(current_thread->log, "Thread " << current_thread->poco_thread_number << " is exiting");
        current_thread->thread_exited = true;
    }
};

ThreadStatusPtr ThreadStatus::getCurrent() const { return current_thread; }

thread_local ThreadStatusPtr current_thread = ThreadStatus::create();

/// Order of current_thread and current_thread_scope matters
static thread_local ScopeCurrentThread current_thread_scope;




}
