#include "common/sleep.h"

#include <time.h>
#include <errno.h>

#if defined(OS_DARWIN)
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif

/**
  * Sleep with nanoseconds precision. Tolerant to signal interruptions
  *
  * In case query profiler is turned on, all threads spawned for
  * query execution are repeatedly interrupted by signals from timer.
  * Functions for relative sleep (sleep(3), nanosleep(2), etc.) have
  * problems in this setup and man page for nanosleep(2) suggests
  * using absolute deadlines, for instance clock_nanosleep(2).
  */
void sleepForNanoseconds(uint64_t nanoseconds)
{
#if defined(OS_DARWIN)
    //https://developer.apple.com/library/archive/technotes/tn2169/_index.html
    //https://dshil.github.io/blog/missed-os-x-clock-guide/
    static mach_timebase_info_data_t timebase_info{};
    if (timebase_info.denom == 0)
        mach_timebase_info(&timebase_info);

    uint64_t time_to_wait = nanoseconds * timebase_info.denom / timebase_info.numer;
    uint64_t now = mach_absolute_time();

    while (mach_wait_until(now + time_to_wait) != KERN_SUCCESS);
#else
    constexpr auto clock_type = CLOCK_MONOTONIC;

    struct timespec current_time;
    clock_gettime(clock_type, &current_time);

    constexpr uint64_t resolution = 1'000'000'000;
    struct timespec finish_time = current_time;

    finish_time.tv_nsec += nanoseconds % resolution;
    const uint64_t extra_second = finish_time.tv_nsec / resolution;
    finish_time.tv_nsec %= resolution;

    finish_time.tv_sec += (nanoseconds / resolution) + extra_second;

    while (clock_nanosleep(clock_type, TIMER_ABSTIME, &finish_time, nullptr) == EINTR);
#endif
}

void sleepForMicroseconds(uint64_t microseconds)
{
    sleepForNanoseconds(microseconds * 1000);
}

void sleepForMilliseconds(uint64_t milliseconds)
{
    sleepForMicroseconds(milliseconds * 1000);
}

void sleepForSeconds(uint64_t seconds)
{
    sleepForMilliseconds(seconds * 1000);
}
