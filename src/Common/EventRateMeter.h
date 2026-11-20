#pragma once

#include <base/defines.h>

#include <Common/ExponentiallySmoothedCounter.h>


namespace DB
{

/// Event count measurement with exponential smoothing intended for computing time derivatives
class EventRateMeter
{
public:
    explicit EventRateMeter(double now, double period_, size_t heating_ = 0)
        : period(period_)
        , max_interval(period * 10)
        , heating(heating_)
    {
        reset(now);
    }

    /// Add `count` events happened at `now` instant.
    /// Previous events that are older than `period` from `now` will be forgotten
    /// in a way to keep average event rate the same, using exponential smoothing.
    /// NOTE: Adding events into distant past (further than `period`) must be avoided.
    void add(double now, double count)
    {
        // Remove data for initial heating stage that can present at the beginning of a query.
        // Otherwise it leads to wrong gradual increase of average value, turning algorithm into not very reactive.
        if (count != 0.0 && data_points++ <= heating)
            reset(events.time, data_points);

        duration.add(std::min(max_interval, now - duration.time), now, period);
        events.add(count, now, period);
    }

    /// Compute average event rate throughout `[now - period, now]` period.
    /// If measurements are just started (`now - period < start`), then average
    /// is computed based on shorter `[start; now]` period to avoid initial linear growth.
    double rate(double now)
    {
        add(now, 0);
        if (unlikely(now <= start))
            return 0;

        // We do not use .get() because sum of weights will anyway be canceled out (optimization)
        return events.value / duration.value;
    }

    void reset(double now, size_t data_points_ = 0)
    {
        start = now;
        events = ExponentiallySmoothedAverage();
        duration = ExponentiallySmoothedAverage();
        data_points = data_points_;
    }

private:
    const double period;
    const double max_interval;
    const size_t heating;
    double start; // Instant in past without events before it; when measurement started or reset
    ExponentiallySmoothedAverage duration; // Current duration of a period
    ExponentiallySmoothedAverage events; // Estimated number of events in last `duration` seconds
    size_t data_points = 0;
};

}
