#include <Processors/QueryPlan/ConcurrencyProfile.h>
#include <base/defines.h>

#include <algorithm>
#include <queue>

namespace DB
{

ConcurrencyProfile::ConcurrencyProfile(const WorkIntervalsPerThread & intervals_per_thread)
{
    /// Cursor into one thread's run: the interval, and whether its start or its end comes next.
    /// A thread's intervals do not overlap, so its events are already in chronological order.
    struct Cursor
    {
        size_t thread;
        size_t position;
        bool is_end;
    };

    const auto event_time = [&](const Cursor & cursor)
    {
        const auto & interval = intervals_per_thread[cursor.thread][cursor.position];
        return cursor.is_end
            ? interval.start_of_interval_ns + interval.duration_of_interval_ns
            : interval.start_of_interval_ns;
    };

    /// Min-heap over the next event of each thread. At equal times intervals are closed before new
    /// ones are opened, so that touching intervals never look like two concurrent ones.
    const auto later_event = [&](const Cursor & lhs, const Cursor & rhs)
    {
        const UInt64 lhs_time = event_time(lhs);
        const UInt64 rhs_time = event_time(rhs);
        if (lhs_time != rhs_time)
            return lhs_time > rhs_time;
        return !lhs.is_end && rhs.is_end;
    };

    size_t total_events = 0;
    std::vector<Cursor> initial;
    for (size_t thread = 0; thread < intervals_per_thread.size(); ++thread)
    {
        if (intervals_per_thread[thread].empty())
            continue;

        initial.push_back({thread, 0, false});
        total_events += 2 * intervals_per_thread[thread].size();
    }

    std::priority_queue<Cursor, std::vector<Cursor>, decltype(later_event)> heads(later_event, std::move(initial));

    if (heads.empty())
        return;

    times.reserve(total_events);
    concurrency.reserve(total_events);
    busy_integral.reserve(total_events);

    times.push_back(event_time(heads.top()));
    concurrency.push_back(0);
    busy_integral.push_back(0);

    UInt64 running = 0;
    while (!heads.empty())
    {
        Cursor cursor = heads.top();
        heads.pop();

        const UInt64 time = event_time(cursor);

        if (time != times.back())
        {
            busy_integral.push_back(busy_integral.back() + concurrency.back() * (time - times.back()));
            times.push_back(time);
            concurrency.push_back(running);
        }

        if (cursor.is_end)
            --running;
        else
            ++running;

        concurrency.back() = running;

        if (cursor.is_end)
            ++cursor.position;

        cursor.is_end = !cursor.is_end;

        if (cursor.position < intervals_per_thread[cursor.thread].size())
            heads.push(cursor);
    }
}

UInt64 ConcurrencyProfile::busyTimeIn(const TimeIntervals & intervals) const
{
    UInt64 busy_ns = 0;
    for (const auto & interval : intervals)
        busy_ns += integralAt(interval.end) - integralAt(interval.start);
    return busy_ns;
}

UInt64 ConcurrencyProfile::integralAt(UInt64 time) const
{
    if (times.empty() || time <= times.front())
        return 0;

    if (time >= times.back())
        return busy_integral.back();

    const size_t segment = std::upper_bound(times.begin(), times.end(), time) - times.begin() - 1;
    return busy_integral[segment] + concurrency[segment] * (time - times[segment]);
}

}
