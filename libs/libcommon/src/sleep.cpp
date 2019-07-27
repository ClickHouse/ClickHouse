#include "common/sleep.h"

#include <time.h>
#include <errno.h>

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
