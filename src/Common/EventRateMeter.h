#pragma once

#include <base/types.h>
#include <algorithm>
#include <cmath>

namespace DB
{

/// Event count measurement with exponential smoothing intended for computing time derivatives
class EventRateMeter {
public:
    explicit EventRateMeter(UInt64 period_, UInt64 resolution = 1000)
        : period(std::max(period_, 1ul))
        , step(std::max(period / resolution, 1ul))
        , decay(1.0 - 1.0 / resolution)
    {}

    /// Add `count` events happened at `now` instant.
    /// Previous events that are older than `period` from `now` will be forgotten
    /// in a way to keep average event rate the same, using exponential smoothing.
    /// NOTE: Adding events into distant past (further than `period`) must be avoided.
    void add(UInt64 now, UInt64 count)
    {
        if (unlikely(end == 0))
        {
            // Initialization during the first call
            end = now + period;
        }
        else if (now > end)
        {
            // Compute number of steps we have to move for `now <= end` to became true
            UInt64 steps = (now - end + step - 1) / step;
            end += steps * step;
            assert(now <= end);

            // Forget old events, assuming all events are distributed evenly throughout whole `period`.
            // This assumption leads to exponential decay in case no new events will come.
            if (steps == 1)
                events *= decay;
            else
                events *= std::pow(decay, steps);
        }

        // Add new events
        events += count;
    }

    /// Compute average event rate thoughout `[now - period, now]` period.
    double rate(UInt64 now)
    {
        add(now, 0);
        return double(events) / period;
    }

    void reset()
    {
        events = 0;
        end = 0;
    }

private:
    const UInt64 period;
    const UInt64 step;
    const double decay;
    double events = 0; // Estimated number of events in [now - period, now] range
    UInt64 end = 0;
};

}
