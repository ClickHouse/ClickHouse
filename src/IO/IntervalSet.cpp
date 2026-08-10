#include <IO/IntervalSet.h>

#include <algorithm>

namespace DB
{

void IntervalSet::add(ByteRange r)
{
    if (r.size == 0)
        return;

    size_t new_start = r.offset;
    size_t new_end = r.end();

    auto erase_from = intervals.begin();
    while (erase_from != intervals.end() && erase_from->end() < new_start)
        ++erase_from;

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

VectorWithMemoryTracking<ByteRange> IntervalSet::subtract(ByteRange r) const
{
    VectorWithMemoryTracking<ByteRange> out;
    if (r.size == 0)
        return out;
    size_t cur = r.offset;
    size_t end = r.end();
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

VectorWithMemoryTracking<ByteRange> IntervalSet::intersect(ByteRange r) const
{
    VectorWithMemoryTracking<ByteRange> out;
    if (r.size == 0)
        return out;
    const size_t rs = r.offset;
    const size_t re = r.end();
    for (const auto & i : intervals)
    {
        if (i.end() <= rs)
            continue;
        if (i.offset >= re)
            break;
        const size_t lo = std::max(rs, i.offset);
        const size_t hi = std::min(re, i.end());
        out.push_back({lo, hi - lo});
    }
    return out;
}

void IntervalSet::remove(ByteRange r)
{
    if (r.size == 0)
        return;
    const size_t rs = r.offset;
    const size_t re = r.end();

    VectorWithMemoryTracking<ByteRange> next;
    for (const auto & i : intervals)
    {
        if (i.end() <= rs || i.offset >= re)
        {
            next.push_back(i);   /// no overlap, keep as-is
            continue;
        }
        /// Overlap: keep the parts of `i` outside `r` (left and/or right), in order.
        if (i.offset < rs)
            next.push_back({i.offset, rs - i.offset});
        if (i.end() > re)
            next.push_back({re, i.end() - re});
    }
    intervals = std::move(next);
}

std::optional<ByteRange> IntervalSet::coveringInterval(size_t pos) const
{
    auto it = std::upper_bound(intervals.begin(), intervals.end(), pos,
        [](size_t p, const ByteRange & r) { return p < r.offset; });
    if (it == intervals.begin())
        return std::nullopt;
    --it;
    if (pos < it->end())
        return *it;
    return std::nullopt;
}

std::optional<ByteRange> IntervalSet::nextIntervalAfter(size_t pos) const
{
    auto it = std::upper_bound(intervals.begin(), intervals.end(), pos,
        [](size_t p, const ByteRange & r) { return p < r.offset; });
    if (it == intervals.end())
        return std::nullopt;
    return *it;
}

size_t IntervalSet::contiguousEnd(size_t pos, size_t bridge_gap) const
{
    /// Start from the interval covering `pos`, or the next one when the gap to
    /// it is narrow (bridgeable).
    auto it = std::upper_bound(intervals.begin(), intervals.end(), pos,
        [](size_t p, const ByteRange & r) { return p < r.offset; });
    if (it != intervals.begin() && pos < std::prev(it)->end())
        --it;
    else if (it == intervals.end() || it->offset - pos >= bridge_gap)
        return pos;

    size_t end = it->end();
    for (++it; it != intervals.end() && it->offset - end < bridge_gap; ++it)
        end = it->end();
    return end;
}

size_t IntervalSet::contiguousStart(size_t pos, size_t bridge_gap) const
{
    auto it = std::upper_bound(intervals.begin(), intervals.end(), pos,
        [](size_t p, const ByteRange & r) { return p < r.offset; });
    if (it == intervals.begin())
        return pos;
    --it;
    /// `pos` past the interval by a wide gap: no demand run here.
    if (pos >= it->end() && pos - it->end() >= bridge_gap)
        return pos;

    size_t start = it->offset;
    while (it != intervals.begin())
    {
        auto prev = std::prev(it);
        if (start - prev->end() >= bridge_gap)
            break;
        start = prev->offset;
        it = prev;
    }
    return std::min(start, pos);
}

size_t IntervalSet::totalBytes() const
{
    size_t total = 0;
    for (const auto & i : intervals)
        total += i.size;
    return total;
}

}
