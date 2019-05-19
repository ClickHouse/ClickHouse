#include "Sleep.h"

#include <iostream>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void SleepForNanoseconds(UInt64 nanoseconds)
{
    const auto clock_type = CLOCK_REALTIME;

    struct timespec current_time;
    clock_gettime(clock_type, &current_time);

    const UInt64 resolution = 1'000'000'000;
    struct timespec finish_time = current_time;

    finish_time.tv_nsec += nanoseconds % resolution;
    const UInt64 extra_second = finish_time.tv_nsec / resolution;
    finish_time.tv_nsec %= resolution;

    finish_time.tv_sec += (nanoseconds / resolution) + extra_second;

    while (clock_nanosleep(clock_type, TIMER_ABSTIME, &finish_time, nullptr) == EINTR);
}

void SleepForMicroseconds(UInt64 microseconds)
{
    SleepForNanoseconds(microseconds * 1000);
}

void SleepForMilliseconds(UInt64 milliseconds)
{
    SleepForMicroseconds(milliseconds * 1000);
}

void SleepForSeconds(UInt64 seconds)
{
    SleepForMilliseconds(seconds * 1000);
}
}
