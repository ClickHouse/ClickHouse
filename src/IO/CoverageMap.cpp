#include <IO/CoverageMap.h>

#include <algorithm>

namespace DB
{

CoverageMap::Resident CoverageMap::residentAt(size_t offset) const
{
    for (size_t i = 0; i < entries.size(); ++i)
        for (const auto & r : entries[i].resident)
            if (offset >= r.offset && offset < r.end())
                return {i, entries[i].tier, r.end()};
    return {};
}

size_t CoverageMap::nextGapStart(size_t from) const
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

size_t CoverageMap::gapEnd(size_t gap_start) const
{
    size_t end = plan_end;
    for (const auto & entry : entries)
        for (const auto & r : entry.resident)
            if (r.offset > gap_start && r.offset < end)
                end = r.offset;
    return end;
}

ByteRange CoverageMap::fetchWindowAt(ByteRange req) const
{
    if (req.size == 0)
        return req;
    size_t lo = req.offset;
    size_t hi = req.end();
    for (const auto & entry : entries)
        for (const auto & m : entry.aligned_miss)
        {
            /// Head: `req` starts inside this miss cell -> open at the cell's start.
            if (m.offset <= req.offset && req.offset < m.end())
                lo = std::min(lo, m.offset);
            /// Tail: `req` ends inside this miss cell -> run to the cell's end.
            if (m.offset < req.end() && req.end() <= m.end())
                hi = std::max(hi, m.end());
        }
    return ByteRange{lo, hi - lo};
}

size_t CoverageMap::streamReach(size_t from, size_t min_gap) const
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
        /// where the connection stops, not bridged). Strictly below `min_gap` - a run
        /// of exactly `min_gap` reopens, matching `LongConnection::canContinue`.
        if (r.run_end - pos < min_gap && r.run_end < plan_end)
        {
            pos = r.run_end;
            continue;
        }
        break;
    }
    return pos;
}

}
