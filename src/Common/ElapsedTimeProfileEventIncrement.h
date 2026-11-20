#pragma once

#include <Common/ProfileEvents.h>
#include <Common/CurrentThread.h>
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
        : event(event_), watch(CLOCK_MONOTONIC), counters(CurrentThread::getProfileEvents())
    {
        counters.preallocate(event);
        watch.restart();
    }

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
        counters.incrementNonAllocating(event, elapsed());
    }

    const ProfileEvents::Event event;
    Stopwatch watch;

private:
    /// The scope that starts the timer must outlive it, just as for `ProfileEvents::Timer`.
    ProfileEvents::Counters & counters;
};

}
