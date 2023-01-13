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

template <Time time>
struct ProfileEventTimeIncrement
{
    explicit ProfileEventTimeIncrement<time>(ProfileEvents::Event event_)
        : event(event_), watch(CLOCK_MONOTONIC) {}

    ~ProfileEventTimeIncrement()
    {
        watch.stop();
        if constexpr (time == Time::Nanoseconds)
            ProfileEvents::increment(event, watch.elapsedNanoseconds());
        else if constexpr (time == Time::Microseconds)
            ProfileEvents::increment(event, watch.elapsedMicroseconds());
        else if constexpr (time == Time::Milliseconds)
            ProfileEvents::increment(event, watch.elapsedMilliseconds());
        else if constexpr (time == Time::Seconds)
            ProfileEvents::increment(event, watch.elapsedSeconds());
    }

    ProfileEvents::Event event;
    Stopwatch watch;
};

}
