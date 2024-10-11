#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace DB
{

enum Time
{
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
};

template <Time unit>
struct ProfileEventTimeIncrement
{
    explicit ProfileEventTimeIncrement(ProfileEvents::Event event_)
        : event(event_), watch(CLOCK_MONOTONIC) {}

    UInt64 elapsed()
    {
        if constexpr (unit == Time::Nanoseconds)
            return watch.elapsedNanoseconds();
        else if constexpr (unit == Time::Microseconds)
            return watch.elapsedMicroseconds();
        else if constexpr (unit == Time::Milliseconds)
            return watch.elapsedMilliseconds();
        else if constexpr (unit == Time::Seconds)
            return watch.elapsedSeconds();
    }

    ~ProfileEventTimeIncrement()
    {
        watch.stop();
        ProfileEvents::increment(event, elapsed());
    }

    ProfileEvents::Event event;
    Stopwatch watch;
};

}
