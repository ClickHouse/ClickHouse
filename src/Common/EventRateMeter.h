#pragma once

#include <base/defines.h>

#include <Common/ExponentiallySmoothedCounter.h>

#include <numbers>


namespace DB
{

/// Event count measurement with exponential smoothing intended for computing time derivatives
class EventRateMeter
{
public:
    explicit EventRateMeter(double now, double period_)
        : period(period_)
        , half_decay_time(period * std::numbers::ln2) // for `ExponentiallySmoothedAverage::sumWeights()` to be equal to `1/period`
    {
        reset(now);
    }

    /// Add `count` events happened at `now` instant.
    /// Previous events that are older than `period` from `now` will be forgotten
    /// in a way to keep average event rate the same, using exponential smoothing.
    /// NOTE: Adding events into distant past (further than `period`) must be avoided.
    void add(double now, double count)
    {
        if (now - period <= start) // precise counting mode
            events = ExponentiallySmoothedAverage(events.value + count, now);
        else // exponential smoothing mode
            events.add(count, now, half_decay_time);
    }

    /// Compute average event rate throughout `[now - period, now]` period.
    /// If measurements are just started (`now - period < start`), then average
    /// is computed based on shorter `[start; now]` period to avoid initial linear growth.
    double rate(double now)
    {
        add(now, 0);
        if (unlikely(now <= start))
            return 0;
        if (now - period <= start) // precise counting mode
            return events.value / (now - start);
        else // exponential smoothing mode
            return events.get(half_decay_time); // equals to `events.value / period`
    }

    void reset(double now)
    {
        start = now;
        events = ExponentiallySmoothedAverage();
    }

private:
    const double period;
    const double half_decay_time;
    double start; // Instant in past without events before it; when measurement started or reset
    ExponentiallySmoothedAverage events; // Estimated number of events in the last `period`
};

}
