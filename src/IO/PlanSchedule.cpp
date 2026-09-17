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

VectorWithMemoryTracking<ByteRange> sortAndMerge(VectorWithMemoryTracking<ByteRange> parts)
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

/// The fill closure: GAP-driven. Walk the span; each gap (a range no tier
/// holds) is aligned out to the cache cells it must fill via `fetchWindowAt` -
/// the same window the executor fetches - which pulls in the before/after
/// alignment slack. A span byte resident in a faster tier induces NO fetch
/// and NO slack (it is served from that tier, not fetched).
VectorWithMemoryTracking<ByteRange> fillRegion(const CoverageMap & g, ByteRange span)
{
    VectorWithMemoryTracking<ByteRange> parts;
    size_t pos = span.offset;
    const size_t end = span.offset + span.size;
    while (pos < end)
    {
        const auto res = g.residentAt(pos);
        if (res.resident())
        {
            pos = std::min(res.run_end, end);  /// resident span bytes: served, not fetched
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
    return sortAndMerge(std::move(parts));
}

/// The source-fetch runs of one fill part: `part` minus every plan-resident region (a resident
/// region is served from its tier, never SCHEDULED as a source read - whether the
/// executor reads through one at run time is a display-state decision, not a schedule property:
/// see the drain-loop's frontier-in-hole read-through). Beyond the plan span the geometry has no
/// residency info (`gapEnd` returns `plan_end`), so the remainder - the fill closure's
/// after-slack - extends the run.
VectorWithMemoryTracking<ByteRange> fetchRunsFor(const CoverageMap & g, ByteRange part)
{
    VectorWithMemoryTracking<ByteRange> runs;
    size_t pos = part.offset;
    const size_t end = part.end();
    while (pos < end)
    {
        const auto res = g.residentAt(pos);
        if (res.resident())
        {
            pos = std::min(res.run_end, end);
            continue;
        }
        size_t run_end = std::min(g.gapEnd(pos), end);
        if (run_end <= pos)
            run_end = end;   /// past the plan span: no residency info
        if (!runs.empty() && runs.back().end() == pos)
            runs.back().size = run_end - runs.back().offset;
        else
            runs.push_back(ByteRange{pos, run_end - pos});
        pos = run_end;
    }
    return runs;
}

/// The cells connection `conn` populates. A cell holding USER (consumed) bytes is
/// filled in EVERY tier that misses it - the source read fills the whole chain at
/// once. A slack-only cell is filled ONLY in the tier that owns it - the coarsest-
/// alignment tier missing it, the one whose segment alignment created the slack.
VectorWithMemoryTracking<PlanSchedule::WriteTarget> writeTargetsFor(
    const CoverageMap & g, ByteRange conn, ByteRange span)
{
    VectorWithMemoryTracking<PlanSchedule::WriteTarget> targets;
    for (size_t ei = 0; ei < g.entries.size(); ++ei)
    {
        const auto & e = g.entries[ei];
        /// A whole-cell tier (page, first-writer-wins) is fillable only if the
        /// connection covers the whole cell; an incremental tier (fs) appends
        /// whatever prefix the connection covers.
        const bool whole_block = e.whole_cell;
        for (const auto & m : e.aligned_miss)
        {
            if (whole_block ? !contains(conn, m) : !overlaps(conn, m))
                continue;

            const bool holds_user = overlaps(m, span);
            if (holds_user)
            {
                /// A consumed cell is filled in EVERY tier that misses it, straight from the
                /// source read - the whole cache chain is populated at the fetch, no serve-front
                /// promotion.
                targets.push_back({ei, m, whole_block});
                continue;
            }

            /// Slack-only cell: own it iff no tier misses the same bytes with a
            /// strictly WIDER cell - the coarsest-cell tier's alignment created
            /// the slack, so it owns the fill.
            bool owns = true;
            for (size_t ej = 0; owns && ej < g.entries.size(); ++ej)
            {
                if (ej == ei)
                    continue;
                for (const auto & m2 : g.entries[ej].aligned_miss)
                    if (overlaps(m2, m) && m2.size > m.size)
                    {
                        owns = false;
                        break;
                    }
            }
            if (owns)
                targets.push_back({ei, m, whole_block});
        }
    }
    return targets;
}

}

PlanSchedule buildSchedule(
    const CoverageMap & geometry,
    size_t serve_window_bytes,
    size_t serve_block_bytes)
{
    PlanSchedule sched;
    if (geometry.plan_end <= geometry.plan_start)
        return sched;

    /// The span IS the request: everything within it is read by the scan (User);
    /// only the cell slack around it is FillOnly.
    const ByteRange span{geometry.plan_start, geometry.plan_end - geometry.plan_start};

    const auto fill = fillRegion(geometry, span);

    /// --- ranges: typed decomposition of span ∪ fill closure ---
    /// Decompose span ∪ fill (the span plus the cells' overhang), breaking at
    /// every residency boundary (the granularity `Display::read` serves at) and
    /// at the span edges (where purpose flips between FillOnly and User).
    {
        VectorWithMemoryTracking<ByteRange> walk_parts;
        walk_parts.push_back(span);
        for (const auto & f : fill)
            walk_parts.push_back(f);

        for (const auto & piece : sortAndMerge(std::move(walk_parts)))
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

                if (pos < span.offset)
                    seg_end = std::min(seg_end, span.offset);
                else if (pos < span.end())
                    seg_end = std::min(seg_end, span.end());

                const bool is_user = pos >= span.offset && pos < span.end();
                sched.ranges.push_back(PlanSchedule::TypedRange{
                    .range = ByteRange{pos, seg_end - pos},
                    .purpose = is_user ? PlanSchedule::Purpose::User : PlanSchedule::Purpose::FillOnly,
                    .resident = res.resident(),
                    .tier_entry = res.entry,
                });
                pos = seg_end;
            }
        }
    }

    /// --- retrieves: one per fill-closure GAP (pure coverage) ---
    /// The schedule does NOT group gaps into connections - it lists each cache-cell-aligned gap as
    /// its own job. The runtime decides how many source connections span them (a held connection
    /// bridges a small cached hole or reopens at a wide one - see ReaderExecutor's
    /// `scheduleLookaheadReach` / `canContinue`); the schedule only says WHAT to read.
    /// Fetch shaping is the `into` cells themselves: pieces open at cell starts and ceil to
    /// cell ends. An `into`-empty (bypass) job reads exactly the requested bytes - nothing
    /// could hold an extension, and the serve would fetch-and-discard it every window.
    for (const auto & f : fill)
    {
        PlanSchedule::Retrieve r;
        r.range = f;
        r.into = writeTargetsFor(geometry, f, span);
        r.fetch_runs = fetchRunsFor(geometry, f);
        sched.retrieves.push_back(std::move(r));
    }

    /// --- steps: what each readNextWindow returns, wired to its retrieve ---
    size_t cursor = span.offset;
    const size_t span_end = span.offset + span.size;
    while (cursor < span_end)
    {
        const auto res = geometry.residentAt(cursor);
        /// A resident step spans the maximal CONTIGUOUS resident region across ALL tiers
        /// (`nextGapStart`) - the serve (`serveFromDisplay`/`Display::read`) streams
        /// adjacent resident runs of different tiers into one window. `res.run_end`
        /// stops at the tier-run boundary and would split one served window into
        /// several steps.
        size_t out_end = res.resident() ? geometry.nextGapStart(cursor) : geometry.gapEnd(cursor);
        out_end = std::min(out_end, span_end);
        const ByteRange out{cursor, out_end - cursor};

        std::optional<size_t> require;
        if (!res.resident())  /// a gap is served by the retrieve covering it
            for (size_t ri = 0; ri < sched.retrieves.size(); ++ri)
                if (contains(sched.retrieves[ri].range, out))
                {
                    require = ri;
                    break;
                }

        sched.serve_runs.push_back(PlanSchedule::ServeRun{
            .output = out,
            .require_retrieve = require,
            .serve_bound = require.has_value() ? serve_window_bytes : serve_block_bytes});
        cursor = out_end;
    }

    return sched;
}

}
