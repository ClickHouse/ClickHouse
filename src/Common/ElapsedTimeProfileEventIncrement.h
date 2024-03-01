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
    explicit ProfileEventTimeIncrement<time>(ProfileEvents::Event event_)
        : event(event_), watch(CLOCK_MONOTONIC) {}

    template <Time time = unit>
    UInt64 elapsed()
    {
        if constexpr (time == Time::Nanoseconds)
            return watch.elapsedNanoseconds();
        else if constexpr (time == Time::Microseconds)
            return watch.elapsedMicroseconds();
        else if constexpr (time == Time::Milliseconds)
            return watch.elapsedMilliseconds();
        else if constexpr (time == Time::Seconds)
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
