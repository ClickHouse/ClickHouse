#include <IO/ByteRangeSet.h>

#include <algorithm>
#include <iterator>
#include <fmt/format.h>

namespace DB
{

void ByteRangeSet::add(ByteRange range)
{
    if (range.size == 0)
        return;

    size_t new_start = range.offset;
    size_t new_end = range.end();

    auto erase_from = std::partition_point(
        intervals.begin(), intervals.end(), [&](const ByteRange & interval) { return interval.end() < new_start; });

    auto it = erase_from;
    while (it != intervals.end() && it->offset <= new_end)
    {
        new_start = std::min(new_start, it->offset);
        new_end = std::max(new_end, it->end());
        ++it;
    }

    auto insert_pos = intervals.erase(erase_from, it);
    intervals.insert(insert_pos, ByteRange{new_start, new_end - new_start});
}

VectorWithMemoryTracking<ByteRange> ByteRangeSet::subtract(ByteRange range) const
{
    VectorWithMemoryTracking<ByteRange> out;
    if (range.size == 0)
        return out;
    size_t cur = range.offset;
    size_t end = range.end();
    auto it = std::partition_point(
        intervals.begin(), intervals.end(), [&](const ByteRange & interval) { return interval.end() <= cur; });
    for (; it != intervals.end() && it->offset < end; ++it)
    {
        if (it->offset > cur)
            out.push_back({cur, it->offset - cur});
        cur = std::max(cur, it->end());
        if (cur >= end)
            break;
    }
    if (cur < end)
        out.push_back({cur, end - cur});
    return out;
}

void ByteRangeSet::remove(ByteRange range)
{
    if (range.size == 0)
        return;
    const size_t rs = range.offset;
    const size_t re = range.end();

    VectorWithMemoryTracking<ByteRange> next;
    for (const auto & i : intervals)
    {
        if (i.end() <= rs || i.offset >= re)
        {
            next.push_back(i);   /// no overlap, keep as-is
            continue;
        }
        /// Overlap: keep the parts of `i` outside `range` (left and/or right), in order.
        if (i.offset < rs)
            next.push_back({i.offset, rs - i.offset});
        if (i.end() > re)
            next.push_back({re, i.end() - re});
    }
    intervals = std::move(next);
}

ByteRangeSet ByteRangeSet::intersect(ByteRange range) const
{
    ByteRangeSet out;
    if (range.size == 0)
        return out;
    auto it = std::partition_point(
        intervals.begin(), intervals.end(), [&](const ByteRange & interval) { return interval.end() <= range.offset; });
    for (; it != intervals.end() && it->offset < range.end(); ++it)
    {
        const size_t begin = std::max(it->offset, range.offset);
        const size_t end = std::min(it->end(), range.end());
        out.intervals.push_back({begin, end - begin});
    }
    return out;
}

void ByteRangeSet::shift(size_t delta)
{
    for (auto & i : intervals)
        i.offset += delta;
}

size_t ByteRangeSet::totalBytes() const
{
    size_t total = 0;
    for (const auto & i : intervals)
        total += i.size;
    return total;
}

String ByteRangeSet::describe() const
{
    static constexpr size_t max_ranges_in_full = 9;
    static constexpr size_t ranges_at_each_end = 4;

    String result = fmt::format("{} ranges", intervals.size());
    auto append = [&](size_t i)
    {
        fmt::format_to(std::back_inserter(result), "{}[{}, {})", i == 0 ? ": " : ", ", intervals[i].offset, intervals[i].end());
    };

    if (intervals.size() <= max_ranges_in_full)
    {
        for (size_t i = 0; i < intervals.size(); ++i)
            append(i);
        return result;
    }

    for (size_t i = 0; i < ranges_at_each_end; ++i)
        append(i);
    result += ", ...";
    for (size_t i = intervals.size() - ranges_at_each_end; i < intervals.size(); ++i)
        append(i);
    return result;
}

}
