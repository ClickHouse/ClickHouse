#pragma once

#include <Core/Types.h>
#include <Common/ProfileEvents.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <pthread.h>


#if defined(__linux__)
#include <linux/taskstats.h>
#else
struct taskstats {};
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
    extern const Event VoluntaryContextSwitches;
    extern const Event InvoluntaryContextSwitches;

#if defined(__linux__)
    extern const Event OSIOWaitMicroseconds;
    extern const Event OSCPUWaitMicroseconds;
    extern const Event OSCPUVirtualTimeMicroseconds;
    extern const Event OSReadChars;
    extern const Event OSWriteChars;
    extern const Event OSReadBytes;
    extern const Event OSWriteBytes;
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


inline UInt64 getCurrentTimeNanoseconds(clockid_t clock_type = CLOCK_MONOTONIC)
{
    struct timespec ts;
    clock_gettime(clock_type, &ts);
    return ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}


struct RUsageCounters
{
    /// In nanoseconds
    UInt64 real_time = 0;
    UInt64 user_time = 0;
    UInt64 sys_time = 0;

    UInt64 soft_page_faults = 0;
    UInt64 hard_page_faults = 0;

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
    }

    static RUsageCounters zeros(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        RUsageCounters res;
        res.real_time = real_time_;
        return res;
    }

    static RUsageCounters current(UInt64 real_time_ = getCurrentTimeNanoseconds())
    {
        ::rusage rusage {};
#if !defined(__APPLE__)
        ::getrusage(RUSAGE_THREAD, &rusage);
#endif
        return RUsageCounters(rusage, real_time_);
    }

    static void incrementProfileEvents(const RUsageCounters & prev, const RUsageCounters & curr, ProfileEvents::Counters & profile_events)
    {
        profile_events.increment(ProfileEvents::RealTimeMicroseconds,   (curr.real_time - prev.real_time) / 1000U);
        profile_events.increment(ProfileEvents::UserTimeMicroseconds,   (curr.user_time - prev.user_time) / 1000U);
        profile_events.increment(ProfileEvents::SystemTimeMicroseconds, (curr.sys_time - prev.sys_time) / 1000U);

        profile_events.increment(ProfileEvents::SoftPageFaults, curr.soft_page_faults - prev.soft_page_faults);
        profile_events.increment(ProfileEvents::HardPageFaults, curr.hard_page_faults - prev.hard_page_faults);
    }

    static void updateProfileEvents(RUsageCounters & last_counters, ProfileEvents::Counters & profile_events)
    {
        auto current_counters = current();
        incrementProfileEvents(last_counters, current_counters, profile_events);
        last_counters = current_counters;
    }
};

#if defined(__linux__)

class TasksStatsCounters
{
public:
    static bool checkIfAvailable();
    static std::unique_ptr<TasksStatsCounters> create(const UInt64 tid);

    void reset();
    void updateCounters(ProfileEvents::Counters & profile_events);

private:
    ::taskstats stats;  //-V730_NOINIT
    std::function<::taskstats()> stats_getter;

    enum class MetricsProvider
    {
        None,
        Procfs,
        Netlink
    };

private:
    explicit TasksStatsCounters(const UInt64 tid, const MetricsProvider provider);

    static MetricsProvider findBestAvailableProvider();
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
