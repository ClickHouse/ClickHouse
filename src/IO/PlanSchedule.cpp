#include <IO/PlanSchedule.h>

#include <algorithm>

namespace DB
{

namespace
{

VectorWithMemoryTracking<ByteRange> mergeSorted(VectorWithMemoryTracking<ByteRange> parts)
{
    std::sort(parts.begin(), parts.end(),
        [](const ByteRange & a, const ByteRange & b) { return a.offset < b.offset; });
    VectorWithMemoryTracking<ByteRange> merged;
    for (const auto & p : parts)
    {
        if (p.size == 0)
            continue;
        if (!merged.empty() && p.offset <= merged.back().end())
        {
            auto & last = merged.back();
            last.size = std::max(last.end(), p.end()) - last.offset;
        }
        else
            merged.push_back(p);
    }
    return merged;
}

/// The fill closure: GAP-driven. Walk the request; each gap (a range no tier
/// holds) is aligned out to the cache cells it must fill via `fetchWindowAt` -
/// the same window the executor fetches - which pulls in the before/after
/// alignment slack. A request byte resident in a faster tier induces NO fetch
/// and NO slack (it is served from that tier, not fetched). Merged + clamped to
/// the plan.
VectorWithMemoryTracking<ByteRange> fillRegion(const ReadPlanGeometry & g, ByteRange request)
{
    VectorWithMemoryTracking<ByteRange> parts;
    size_t pos = request.offset;
    const size_t end = request.offset + request.size;
    while (pos < end)
    {
        const auto res = g.residentAt(pos);
        if (res.resident())
        {
            pos = std::min(res.run_end, end);  /// resident request bytes: served, not fetched
            continue;
        }
        const size_t gap_end = std::min(g.gapEnd(pos), end);
        const ByteRange fetch = g.fetchWindowAt(ByteRange{pos, gap_end - pos});
        const size_t lo = std::max(fetch.offset, g.plan_start);
        const size_t hi = std::min(fetch.end(), g.plan_end);
        if (hi > lo)
            parts.push_back(ByteRange{lo, hi - lo});
        pos = gap_end;
    }
    return mergeSorted(std::move(parts));
}

}

PlanSchedule describePlan(
    const ReadPlanGeometry & geometry,
    ByteRange request_extent,
    MemoryPressureLevel /*pressure*/,
    size_t /*min_bytes_for_seek*/)
{
    PlanSchedule sched;
    if (geometry.plan_end <= geometry.plan_start)
        return sched;

    /// Clamp the request to the plan span.
    const size_t req_lo = std::max(request_extent.offset, geometry.plan_start);
    const size_t req_hi = std::min(request_extent.end(), geometry.plan_end);
    const ByteRange request = (req_hi > req_lo) ? ByteRange{req_lo, req_hi - req_lo} : ByteRange{req_lo, 0};

    /// --- ranges: typed decomposition of request ∪ fill closure ---
    /// The walk region is the request (served, possibly resident) plus the
    /// gap-driven fill closure (fetched + filled, incl. slack). Decompose it,
    /// breaking at every residency boundary (mirroring `serveCacheBlock` /
    /// `coverWindow` granularity) and at the request boundaries (where purpose
    /// flips between FillOnly and User).
    VectorWithMemoryTracking<ByteRange> walk_parts;
    if (request.size)
        walk_parts.push_back(request);
    for (const auto & f : fillRegion(geometry, request))
        walk_parts.push_back(f);

    for (const auto & piece : mergeSorted(std::move(walk_parts)))
    {
        size_t pos = piece.offset;
        while (pos < piece.end())
        {
            const auto res = geometry.residentAt(pos);
            size_t seg_end = res.resident() ? res.run_end : geometry.gapEnd(pos);
            seg_end = std::min(seg_end, piece.end());

            if (request.size)
            {
                if (pos < request.offset)
                    seg_end = std::min(seg_end, request.offset);
                else if (pos < request.end())
                    seg_end = std::min(seg_end, request.end());
            }

            const bool is_user = request.size && pos >= request.offset && pos < request.end();
            sched.ranges.push_back(PlanSchedule::TypedRange{
                .range = ByteRange{pos, seg_end - pos},
                .purpose = is_user ? PlanSchedule::Purpose::User : PlanSchedule::Purpose::FillOnly,
                .resident = res.resident(),
                .tier_entry = res.entry,
                .tier = res.tier,
            });
            pos = seg_end;
        }
    }

    /// --- steps: what each readNextWindow returns (request only) ---
    /// One step per maximal resident run / gap, matching the executor's
    /// per-window dispatch. `require_retrieve` is wired in a later stage.
    size_t cursor = request.offset;
    const size_t request_end = request.offset + request.size;
    while (cursor < request_end)
    {
        const auto res = geometry.residentAt(cursor);
        size_t out_end = res.resident() ? res.run_end : geometry.gapEnd(cursor);
        out_end = std::min(out_end, request_end);
        sched.steps.push_back(PlanSchedule::Step{
            .output = ByteRange{cursor, out_end - cursor},
            .require_retrieve = std::nullopt,
        });
        cursor = out_end;
    }

    return sched;
}

}
