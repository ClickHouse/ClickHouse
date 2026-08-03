#include <Processors/QueryPlan/TimeIntervals.h>

#include <algorithm>

namespace DB
{

TimeIntervals collapseSortedIntervals(TimeIntervals sorted)
{
    size_t write = 0;
    for (size_t read = 0; read < sorted.size(); ++read)
    {
        if (write > 0 && sorted[read].start <= sorted[write - 1].end)
            sorted[write - 1].end = std::max(sorted[write - 1].end, sorted[read].end);
        else
            sorted[write++] = sorted[read];
    }
    sorted.resize(write);
    return sorted;
}

TimeIntervals mergeSortedIntervals(const std::vector<TimeIntervals> & sorted_sequences)
{
    /// Cursor into one input sequence: which sequence, and the next unread position in it.
    struct Cursor
    {
        size_t sequence;
        size_t position;
    };

    size_t total = 0;
    for (const auto & sequence : sorted_sequences)
        total += sequence.size();

    /// Min-heap over the current head of each non-empty sequence, ordered by interval start.
    const auto later_start = [&](const Cursor & lhs, const Cursor & rhs)
    {
        return sorted_sequences[lhs.sequence][lhs.position].start
             > sorted_sequences[rhs.sequence][rhs.position].start;
    };

    std::vector<Cursor> heads;
    heads.reserve(sorted_sequences.size());
    for (size_t i = 0; i < sorted_sequences.size(); ++i)
        if (!sorted_sequences[i].empty())
            heads.push_back({i, 0});
    std::make_heap(heads.begin(), heads.end(), later_start);

    TimeIntervals merged;
    merged.reserve(total);
    while (!heads.empty())
    {
        std::pop_heap(heads.begin(), heads.end(), later_start);
        Cursor & head = heads.back();
        merged.push_back(sorted_sequences[head.sequence][head.position]);

        if (++head.position < sorted_sequences[head.sequence].size())
            std::push_heap(heads.begin(), heads.end(), later_start);
        else
            heads.pop_back();
    }

    return merged;
}

TimeIntervals uniteSortedIntervals(const std::vector<TimeIntervals> & sorted_sequences)
{
    return collapseSortedIntervals(mergeSortedIntervals(sorted_sequences));
}

UInt64 totalIntervalsLength(const TimeIntervals & intervals)
{
    UInt64 total_length_ns = 0;
    for (const auto & interval : intervals)
        total_length_ns += interval.end - interval.start;
    return total_length_ns;
}

}
