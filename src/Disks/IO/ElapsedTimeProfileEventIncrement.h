#pragma once

#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

namespace DB
{

struct ElapsedUSProfileEventIncrement
{
    explicit ElapsedUSProfileEventIncrement(ProfileEvents::Event event_)
        : event(event_), watch((CLOCK_MONOTONIC)) {}

    ~ElapsedUSProfileEventIncrement()
    {
        watch.stop();
        ProfileEvents::increment(event, watch.elapsedMicroseconds());
    }

    ProfileEvents::Event event;
    Stopwatch watch;
};

}
