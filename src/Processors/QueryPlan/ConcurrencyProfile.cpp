#include <Processors/QueryPlan/ConcurrencyProfile.h>

#include <algorithm>

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
    std::vector<Cursor> heads;
    heads.reserve(intervals_per_thread.size());
    for (size_t thread = 0; thread < intervals_per_thread.size(); ++thread)
    {
        if (intervals_per_thread[thread].empty())
            continue;

        heads.push_back({thread, 0, false});
        total_events += 2 * intervals_per_thread[thread].size();
    }
    std::make_heap(heads.begin(), heads.end(), later_event);

    times.reserve(total_events);
    concurrency.reserve(total_events);
    busy_integral.reserve(total_events);

    UInt64 running = 0;
    while (!heads.empty())
    {
        std::pop_heap(heads.begin(), heads.end(), later_event);
        Cursor cursor = heads.back();
        const UInt64 time = event_time(cursor);

        if (times.empty())
        {
            times.push_back(time);
            concurrency.push_back(0);
            busy_integral.push_back(0);
        }
        else if (time != times.back())
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

        if (!cursor.is_end)
        {
            cursor.is_end = true;
            heads.back() = cursor;
            std::push_heap(heads.begin(), heads.end(), later_event);
        }
        else if (++cursor.position < intervals_per_thread[cursor.thread].size())
        {
            cursor.is_end = false;
            heads.back() = cursor;
            std::push_heap(heads.begin(), heads.end(), later_event);
        }
        else
        {
            heads.pop_back();
        }
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
