#include <IO/PlanSchedule.h>

#include <algorithm>

namespace DB
{

namespace
{

bool overlaps(ByteRange a, ByteRange b)
{
    return a.offset < b.end() && b.offset < a.end();
}

bool contains(ByteRange outer, ByteRange inner)
{
    return inner.offset >= outer.offset && inner.end() <= outer.end();
}

size_t alignOf(const GeometryEntry & e)
{
    return std::max(e.head_align, e.tail_align);
}

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
        /// NOT clamped to the plan span: `fetchWindowAt` extends to whole cache
        /// cells (object-bounded), which may straddle `plan_start`/`plan_end` -
        /// a seek mid-segment, or a slow tier's block wider than the plan. The
        /// executor fetches and fills that whole cell, so the schedule must
        /// carry it as a fill target.
        const ByteRange fetch = g.fetchWindowAt(ByteRange{pos, gap_end - pos});
        if (fetch.size)
            parts.push_back(fetch);
        pos = gap_end;
    }
    return mergeSorted(std::move(parts));
}

/// Connections = fill-closure pieces, merged across a resident hole no larger
/// than `min_bytes_for_seek` (bridged: the hole is over-read on the open GET
/// rather than reopening). A wider hole splits the connections.
VectorWithMemoryTracking<ByteRange> connections(
    const VectorWithMemoryTracking<ByteRange> & fill, size_t min_bytes_for_seek)
{
    VectorWithMemoryTracking<ByteRange> conns;
    for (const auto & piece : fill)
    {
        if (!conns.empty() && piece.offset - conns.back().end() <= min_bytes_for_seek)
            conns.back().size = piece.end() - conns.back().offset;  /// bridge the hole
        else
            conns.push_back(piece);
    }
    return conns;
}

/// The cells connection `conn` populates. A cell holding USER bytes is filled in
/// every tier that misses it (promotion of the request up the chain); a slack-
/// only cell is filled ONLY in the tier that owns it - the coarsest-alignment
/// tier missing it, the one whose segment alignment created the slack - never
/// promoted into a faster tier.
VectorWithMemoryTracking<PlanSchedule::WriteTarget> writeTargetsFor(
    const ReadPlanGeometry & g, ByteRange conn, ByteRange request)
{
    VectorWithMemoryTracking<PlanSchedule::WriteTarget> targets;
    for (size_t ei = 0; ei < g.entries.size(); ++ei)
    {
        const auto & e = g.entries[ei];
        /// A whole-block tier (page, `tail_align > 1`, first-writer-wins) is
        /// fillable only if the connection covers the whole cell; an incremental
        /// tier (fs) appends whatever prefix the connection covers.
        const bool whole_block = e.tail_align > 1;
        for (const auto & m : e.aligned_miss)
        {
            if (whole_block ? !contains(conn, m) : !overlaps(conn, m))
                continue;

            const bool holds_user = request.size && overlaps(m, request);
            if (holds_user)
            {
                targets.push_back({ei, m});
                continue;
            }

            /// Slack-only cell: own it iff no tier missing the same bytes has a
            /// strictly coarser alignment.
            const size_t align_e = alignOf(e);
            bool owns = true;
            for (size_t ej = 0; owns && ej < g.entries.size(); ++ej)
            {
                if (ej == ei)
                    continue;
                for (const auto & m2 : g.entries[ej].aligned_miss)
                    if (overlaps(m2, m) && alignOf(g.entries[ej]) > align_e)
                    {
                        owns = false;
                        break;
                    }
            }
            if (owns)
                targets.push_back({ei, m});
        }
    }
    return targets;
}

}

PlanSchedule describePlan(
    const ReadPlanGeometry & geometry,
    ByteRange request_extent,
    MemoryPressureLevel /*pressure*/,
    size_t min_bytes_for_seek)
{
    PlanSchedule sched;
    if (geometry.plan_end <= geometry.plan_start)
        return sched;

    /// Clamp the request to the plan span.
    const size_t req_lo = std::max(request_extent.offset, geometry.plan_start);
    const size_t req_hi = std::min(request_extent.end(), geometry.plan_end);
    const ByteRange request = (req_hi > req_lo) ? ByteRange{req_lo, req_hi - req_lo} : ByteRange{req_lo, 0};

    const auto fill = fillRegion(geometry, request);

    /// --- ranges: typed decomposition of request ∪ fill closure ---
    /// Decompose request ∪ fill, breaking at every residency boundary (mirroring
    /// `serveCacheBlock`/`coverWindow` granularity) and at the request boundaries
    /// (where purpose flips between FillOnly and User).
    {
        VectorWithMemoryTracking<ByteRange> walk_parts;
        if (request.size)
            walk_parts.push_back(request);
        for (const auto & f : fill)
            walk_parts.push_back(f);

        for (const auto & piece : mergeSorted(std::move(walk_parts)))
        {
            size_t pos = piece.offset;
            while (pos < piece.end())
            {
                const auto res = geometry.residentAt(pos);
                size_t seg_end = res.resident() ? res.run_end : geometry.gapEnd(pos);
                seg_end = std::min(seg_end, piece.end());

                /// Beyond the plan span the geometry has no info: `residentAt`
                /// reports a gap and `gapEnd` returns `plan_end` (<= pos here),
                /// which would stall the walk. The remainder of the piece (the
                /// fill closure's after-slack) is one gap segment.
                if (seg_end <= pos)
                    seg_end = piece.end();

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
    }

    /// --- retrieves: one Remote per connection, plus HandedRope promotes ---
    for (const auto & conn : connections(fill, min_bytes_for_seek))
    {
        PlanSchedule::Retrieve r;
        r.range = conn;
        r.source = PlanSchedule::Source::Remote;
        r.into = writeTargetsFor(geometry, conn, request);
        r.retain_for_serve = request.size && overlaps(conn, request);
        sched.retrieves.push_back(std::move(r));
    }

    /// A User range served from a slower resident tier is promoted UP into the
    /// faster tiers that miss it (HandedRope: the foreground hands the served
    /// rope, no re-read, no remote).
    for (const auto & tr : sched.ranges)
    {
        if (tr.purpose != PlanSchedule::Purpose::User || !tr.resident)
            continue;
        PlanSchedule::Retrieve promote;
        promote.range = tr.range;
        promote.source = PlanSchedule::Source::HandedRope;
        promote.upper_source_tier = tr.tier;
        for (size_t ei = 0; ei < tr.tier_entry && ei < geometry.entries.size(); ++ei)  /// faster tiers only
            for (const auto & m : geometry.entries[ei].aligned_miss)
                if (overlaps(m, tr.range))
                    promote.into.push_back({ei, m});
        if (!promote.into.empty())
            sched.retrieves.push_back(std::move(promote));
    }

    /// A lower-tier fill cell that spans a resident run held by a FASTER tier,
    /// where that run is NOT covered by any Remote connection (a split gap, too
    /// wide to bridge), is filled across the run from the upper cache rather
    /// than over-read from remote - so the append-only lower segment completes
    /// without re-fetching bytes a faster tier already has. A run a connection
    /// DOES cover (a bridged small hole) stays a remote over-read: reopening for
    /// it would cost more than the wasted bytes. Collect the unique lower cells
    /// from the Remote retrieves first (a spanning cell appears in both
    /// connections' `into`).
    VectorWithMemoryTracking<PlanSchedule::WriteTarget> lower_cells;
    for (const auto & r : sched.retrieves)
    {
        if (r.source != PlanSchedule::Source::Remote)
            continue;
        for (const auto & wt : r.into)
        {
            bool seen = false;
            for (const auto & c : lower_cells)
                if (c.entry == wt.entry && c.cell.offset == wt.cell.offset && c.cell.size == wt.cell.size)
                    seen = true;
            if (!seen)
                lower_cells.push_back(wt);
        }
    }
    const auto covered_by_remote = [&](ByteRange sub)
    {
        for (const auto & r : sched.retrieves)
            if (r.source == PlanSchedule::Source::Remote && contains(r.range, sub))
                return true;
        return false;
    };
    for (const auto & cell : lower_cells)
        for (const auto & rr : sched.ranges)
        {
            if (!rr.resident || rr.tier_entry >= cell.entry)
                continue;  /// only a strictly-faster resident tier fills the lower cell
            const size_t lo = std::max(rr.range.offset, cell.cell.offset);
            const size_t hi = std::min(rr.range.end(), cell.cell.end());
            if (lo >= hi)
                continue;
            const ByteRange sub{lo, hi - lo};
            if (covered_by_remote(sub))
                continue;  /// bridged into a connection -> stays a remote over-read
            PlanSchedule::Retrieve up;
            up.range = sub;
            up.source = PlanSchedule::Source::UpperCacheRead;
            up.upper_source_tier = rr.tier;
            up.into.push_back(cell);
            sched.retrieves.push_back(std::move(up));
        }

    /// --- deps: natural-order among retrieves writing the same cell ---
    /// A segment appends at its write frontier, so two retrieves filling the
    /// same (entry, cell) must run in offset order (the spanning-writer case
    /// across split connections).
    for (size_t j = 0; j < sched.retrieves.size(); ++j)
        for (size_t i = 0; i < j; ++i)
        {
            bool shares_cell = false;
            for (const auto & tj : sched.retrieves[j].into)
                for (const auto & ti : sched.retrieves[i].into)
                    if (ti.entry == tj.entry && overlaps(ti.cell, tj.cell))
                        shares_cell = true;
            if (shares_cell && sched.retrieves[i].range.offset <= sched.retrieves[j].range.offset)
                sched.retrieves[j].deps.push_back(i);
        }

    /// --- steps: what each readNextWindow returns, wired to its retrieve ---
    size_t cursor = request.offset;
    const size_t request_end = request.offset + request.size;
    while (cursor < request_end)
    {
        const auto res = geometry.residentAt(cursor);
        size_t out_end = res.resident() ? res.run_end : geometry.gapEnd(cursor);
        out_end = std::min(out_end, request_end);
        const ByteRange out{cursor, out_end - cursor};

        std::optional<size_t> require;
        if (!res.resident())  /// a gap is served by the Remote retrieve covering it
            for (size_t ri = 0; ri < sched.retrieves.size(); ++ri)
                if (sched.retrieves[ri].source == PlanSchedule::Source::Remote
                    && contains(sched.retrieves[ri].range, out))
                {
                    require = ri;
                    break;
                }

        sched.steps.push_back(PlanSchedule::Step{.output = out, .require_retrieve = require});
        cursor = out_end;
    }

    return sched;
}

}
