#include <IO/ByteRangeSet.h>

#include <algorithm>

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
    for (const auto & i : intervals)
    {
        if (i.end() <= cur)
            continue;
        if (i.offset >= end)
            break;
        if (i.offset > cur)
            out.push_back({cur, i.offset - cur});
        cur = std::max(cur, i.end());
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
    for (const auto & i : intervals)
    {
        const size_t begin = std::max(i.offset, range.offset);
        const size_t end = std::min(i.end(), range.end());
        if (begin < end)
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

}
