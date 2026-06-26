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

size_t IntervalSet::totalBytes() const
{
    size_t total = 0;
    for (const auto & i : intervals)
        total += i.size;
    return total;
}

}
