#pragma once

#include <base/types.h>
#include <base/getThreadId.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>
#include <boost/noncopyable.hpp>


#if defined(OS_LINUX)
#include <linux/taskstats.h>
#else
struct taskstats {};
#endif

#if defined(OS_DARWIN)
#include <mach/mach.h>
#include <mach/thread_act.h>
#endif

/** Implement ProfileEvents with statistics about resource consumption of the current thread.
  */

namespace ProfileEvents
{
    extern const Event RealTimeMicroseconds;
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
    extern const Event SoftPageFaults;
    extern const Event HardPageFaults;
#if defined(OS_DARWIN)
    extern const Event OSCPUVirtualTimeMicroseconds;
#endif
}

namespace DB
{

/// Handles overflow
template <typename TUInt>
inline TUInt safeDiff(TUInt prev, TUInt curr)
{
    return curr >= prev ? curr - prev : 0;
}


struct RUsageCounters
{
    /// In nanoseconds
    UInt64 real_time = 0;
    UInt64 user_time = 0;
    UInt64 sys_time = 0;

    UInt64 soft_page_faults = 0;
    UInt64 hard_page_faults = 0;

    UInt64 thread_id = 0;

    RUsageCounters() = default;
    RUsageCounters(const ::rusage & rusage_, UInt64 real_time_)
    {
        set(rusage_, real_time_);
    }

    void set(const ::rusage & rusage, UInt64 real_time_)
    {
        real_time = real_time_;
        user_time = rusage.ru_utime.tv_sec * 1000000000UL + rusage.ru_utime.tv_usec * 1000UL;
        sys_time = rusage.ru_stime.tv_sec * 1000000000UL + rusage.ru_stime.tv_usec * 1000UL;

        soft_page_faults = static_cast<UInt64>(rusage.ru_minflt);
        hard_page_faults = static_cast<UInt64>(rusage.ru_majflt);

        thread_id = getThreadId();
    }

    static RUsageCounters current()
    {
        ::rusage rusage {};
#if defined(OS_DARWIN)
        /// macOS has no RUSAGE_THREAD; read per-thread user/system CPU time from the Mach kernel instead.
        ///
        /// pthread_mach_thread_np(...)   -> the low-level Mach thread port (mach_port_t) for the calling
        ///                                  thread. It returns the thread's existing port without taking a
        ///                                  reference, so there is nothing to mach_port_deallocate afterwards.
        /// thread_info(port, THREAD_BASIC_INFO, ...) -> fills thread_basic_info_data_t, which carries the
        ///                                  thread's accumulated user_time and system_time (each a
        ///                                  seconds/microseconds pair). `count` is an in/out field count,
        ///                                  initialized to THREAD_BASIC_INFO_COUNT (the struct's size in
        ///                                  natural_t words). On success it returns KERN_SUCCESS.
        /// current() always reads the calling thread, so cache the last successful reading per thread.
        /// thread_info on one's own port does not realistically fail, but if it ever did we must not
        /// report a zeroed sample: these CPU counters are monotonic and feed unsigned deltas in
        /// incrementProfileEvents, so a regression to zero would underflow into a huge bogus delta (and
        /// trip its chassert in debug). Carry the previous value forward instead (a fresh thread starts
        /// at zero, which is also its true CPU time).
        thread_local time_value_t last_user_time{};
        thread_local time_value_t last_system_time{};
        thread_basic_info_data_t info{};
        mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
        if (thread_info(pthread_mach_thread_np(pthread_self()), THREAD_BASIC_INFO, reinterpret_cast<thread_info_t>(&info), &count) == KERN_SUCCESS)
        {
            last_user_time = info.user_time;
            last_system_time = info.system_time;
        }
        rusage.ru_utime.tv_sec = last_user_time.seconds;
        rusage.ru_utime.tv_usec = last_user_time.microseconds;
        rusage.ru_stime.tv_sec = last_system_time.seconds;
        rusage.ru_stime.tv_usec = last_system_time.microseconds;
#elif defined(OS_SUNOS)
        ::getrusage(RUSAGE_LWP, &rusage);
#else
        ::getrusage(RUSAGE_THREAD, &rusage);
#endif
        return RUsageCounters(rusage, getClockMonotonic());
    }

    static void incrementProfileEvents(const RUsageCounters & prev, const RUsageCounters & curr, ProfileEvents::Counters & profile_events)
    {
        chassert(prev.thread_id == curr.thread_id);
        /// LONG_MAX is ~106751 days
        chassert(curr.real_time - prev.real_time < LONG_MAX);
        chassert(curr.user_time - prev.user_time < LONG_MAX);
        chassert(curr.sys_time - prev.sys_time < LONG_MAX);

        profile_events.increment(ProfileEvents::RealTimeMicroseconds,   (curr.real_time - prev.real_time) / 1000U);
        profile_events.increment(ProfileEvents::UserTimeMicroseconds,   (curr.user_time - prev.user_time) / 1000U);
        profile_events.increment(ProfileEvents::SystemTimeMicroseconds, (curr.sys_time - prev.sys_time) / 1000U);

        profile_events.increment(ProfileEvents::SoftPageFaults, curr.soft_page_faults - prev.soft_page_faults);
        profile_events.increment(ProfileEvents::HardPageFaults, curr.hard_page_faults - prev.hard_page_faults);

#if defined(OS_DARWIN)
        /// On Linux OSCPUVirtualTimeMicroseconds comes from taskstats (the scheduler's on-CPU "run
        /// virtual" time), which macOS does not provide. user+system CPU time from the Mach kernel is
        /// not exactly the same quantity, but it is the closest available equivalent.
        profile_events.increment(ProfileEvents::OSCPUVirtualTimeMicroseconds,
            (curr.user_time - prev.user_time + curr.sys_time - prev.sys_time) / 1000U);
#endif
    }

    static void updateProfileEvents(RUsageCounters & last_counters, ProfileEvents::Counters & profile_events)
    {
        auto current_counters = current();
        incrementProfileEvents(last_counters, current_counters, profile_events);
        last_counters = current_counters;
    }

private:
    static UInt64 getClockMonotonic()
    {
        struct timespec ts{};
        if (0 != clock_gettime(CLOCK_MONOTONIC, &ts))
            throw std::system_error(std::error_code(errno, std::system_category()));
        return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
    }
};

#if defined(OS_LINUX)

struct PerfEventInfo
{
    // see perf_event.h/perf_type_id enum
    int event_type;
    // see configs in perf_event.h
    int event_config;
    ProfileEvents::Event profile_event;
    std::string settings_name;
};

struct PerfEventValue
{
    UInt64 value = 0;
    UInt64 time_enabled = 0;
    UInt64 time_running = 0;
};

static constexpr size_t NUMBER_OF_RAW_EVENTS = 22;

struct PerfDescriptorsHolder : boost::noncopyable
{
    int descriptors[NUMBER_OF_RAW_EVENTS]{};

    PerfDescriptorsHolder();

    ~PerfDescriptorsHolder();

    void releaseResources();
};

struct PerfEventsCounters
{
    PerfDescriptorsHolder thread_events_descriptors_holder;

    // time_enabled and time_running can't be reset, so we have to store the
    // data from the previous profiling period and calculate deltas to them,
    // to be able to properly account for counter multiplexing.
    PerfEventValue previous_values[NUMBER_OF_RAW_EVENTS]{};


    void initializeProfileEvents(const std::string & events_list);
    void finalizeProfileEvents(ProfileEvents::Counters & profile_events);
    void closeEventDescriptors();
    bool processThreadLocalChanges(const std::string & needed_events_list);


    static std::vector<size_t> eventIndicesFromString(const std::string & events_list);
};

// Perf event creation is moderately heavy, so we create them once per thread and
// then reuse.
extern thread_local PerfEventsCounters current_thread_counters;

#else

// the functionality is disabled when we are not running on Linux.
struct PerfEventsCounters
{
    void initializeProfileEvents(const std::string & /* events_list */) {}
    void finalizeProfileEvents(ProfileEvents::Counters & /* profile_events */) {}
    void closeEventDescriptors() {}
};

extern PerfEventsCounters current_thread_counters;

#endif

#if defined(OS_LINUX)

class TasksStatsCounters
{
public:
    enum class MetricsProvider : uint8_t
    {
        None,
        Procfs,
    };

    static const char * metricsProviderString(MetricsProvider provider);
    static bool checkIfAvailable();
    static MetricsProvider findBestAvailableProvider();

    static std::unique_ptr<TasksStatsCounters> create(UInt64 tid);

    void reset();
    void updateCounters(ProfileEvents::Counters & profile_events);

private:
    ::taskstats stats{};
    std::function<::taskstats()> stats_getter;

    explicit TasksStatsCounters(UInt64 tid, MetricsProvider provider);

    static void incrementProfileEvents(const ::taskstats & prev, const ::taskstats & curr, ProfileEvents::Counters & profile_events);
};

#else

class TasksStatsCounters
{
public:
    static bool checkIfAvailable() { return false; }
    static std::unique_ptr<TasksStatsCounters> create(const UInt64 /*tid*/) { return {}; }

    void reset() {}
    void updateCounters(ProfileEvents::Counters &) {}

private:
    TasksStatsCounters(const UInt64 /*tid*/) {}
};

#endif

}
