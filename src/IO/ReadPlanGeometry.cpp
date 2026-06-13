#include <IO/ReadPlanGeometry.h>

#include <algorithm>

namespace DB
{

ReadPlanGeometry::Resident ReadPlanGeometry::residentAt(size_t offset) const
{
    for (size_t i = 0; i < entries.size(); ++i)
        for (const auto & r : entries[i].resident)
            if (offset >= r.offset && offset < r.end())
                return {i, entries[i].tier, r.end()};
    return {};
}

size_t ReadPlanGeometry::nextGapStart(size_t from) const
{
    size_t pos = std::max(from, plan_start);
    while (pos < plan_end)
    {
        auto r = residentAt(pos);
        if (!r.resident())
            return pos;
        pos = r.run_end;
    }
    return plan_end;
}

size_t ReadPlanGeometry::gapEnd(size_t gap_start) const
{
    size_t end = plan_end;
    for (const auto & entry : entries)
        for (const auto & r : entry.resident)
            if (r.offset > gap_start && r.offset < end)
                end = r.offset;
    return end;
}

ByteRange ReadPlanGeometry::fetchWindowAt(ByteRange req) const
{
    if (req.size == 0)
        return req;
    size_t lo = req.offset;
    size_t hi = req.end();
    for (const auto & entry : entries)
        for (const auto & m : entry.aligned_miss)
        {
            /// Head: `req` starts inside this miss run -> round its offset down to
            /// the tier's grid, clamped to the run start (bounded by `head_align`).
            if (entry.head_align > 1 && m.offset <= req.offset && req.offset < m.end())
            {
                size_t floored = (req.offset / entry.head_align) * entry.head_align;
                lo = std::min(lo, std::max(m.offset, floored));
            }
            /// Tail: `req` ends inside this miss run -> round its end up to the
            /// tier's grid, clamped to the run end. `1` (incremental tiers) never
            /// extends.
            if (entry.tail_align > 1 && m.offset < req.end() && req.end() <= m.end())
            {
                size_t ceiled = ((req.end() + entry.tail_align - 1) / entry.tail_align) * entry.tail_align;
                hi = std::max(hi, std::min(m.end(), ceiled));
            }
        }
    return ByteRange{lo, hi - lo};
}

size_t ReadPlanGeometry::streamReach(size_t from, size_t min_gap) const
{
    size_t pos = std::max(from, plan_start);
    while (pos < plan_end)
    {
        auto r = residentAt(pos);
        if (!r.resident())
        {
            pos = gapEnd(pos);  /// stream across the gap
            continue;
        }
        /// A resident run [pos, run_end): bridge it only if small enough to skip
        /// forward over AND something follows it (a trailing resident run is just
        /// where the connection stops, not bridged).
        if (r.run_end - pos <= min_gap && r.run_end < plan_end)
        {
            pos = r.run_end;
            continue;
        }
        break;
    }
    return pos;
}

}
