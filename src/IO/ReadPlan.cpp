#include <IO/ReadPlan.h>

#include <algorithm>

namespace DB
{

using CacheResolution = ICacheProvider::CacheResolution;

namespace
{

bool overlaps(ByteRange a, ByteRange b)
{
    return a.offset < b.end() && b.offset < a.end();
}

/// The cell of `tier` whose range contains `off`, or null when `off` is past the tier's cells.
const CacheResolution * cellCovering(const PlanTier & tier, size_t off)
{
    for (const auto & cell : tier.cells)
        if (cell.range.offset <= off && off < cell.range.end())
            return &cell;
    return nullptr;
}

/// The smallest offset >= `from` this tier can serve (a hit run, or the committed prefix of a miss
/// segment), or `span_end` when nothing at or after `from` is served - the point a FETCH run must stop
/// so it never overruns bytes a tier already holds.
size_t firstServableAtOrAfter(const PlanTier & tier, size_t from, size_t span_end)
{
    for (const auto & cell : tier.cells)
    {
        if (cell.range.end() <= from)
            continue;
        const size_t base = std::max(cell.range.offset, from);
        if (cell.kind == CacheResolution::Kind::Hit && cell.reader)
            return base;
        /// A miss segment's committed prefix is `[cell.range.offset, committed())`; `base` falls in it
        /// (and is servable from the writer) when the frontier is past `base`.
        if (cell.kind == CacheResolution::Kind::Miss && cell.writer && cell.writer->committed() > base)
            return base;
    }
    return span_end;
}

}

ReadPlan::PlanRun ReadPlan::runAt(size_t offset, size_t max_fetch_ahead) const
{
    PlanRun run;
    run.range = ByteRange{offset, 0};
    if (offset < span_start || offset >= span_end)
        return run;

    /// Fastest tier that serves `offset`: a hit reader, or a miss segment already committed here.
    for (const auto & tier : tiers)
    {
        const CacheResolution * cell = cellCovering(tier, offset);
        if (!cell)
            continue;
        if (cell->kind == CacheResolution::Kind::Hit && cell->reader)
        {
            run.reader = cell->reader.get();
            run.range = ByteRange{offset, cell->range.end() - offset};
            return run;
        }
        if (cell->kind == CacheResolution::Kind::Miss && cell->writer)
        {
            /// `offset` is inside the committed prefix `[cell.range.offset, committed())` (its start is
            /// covered by `cellCovering`), so the writer can serve `[offset, committed())` from cache.
            const size_t committed = cell->writer->committed();
            if (offset < committed)
            {
                run.writer = cell->writer.get();
                run.range = ByteRange{offset, std::min(committed, cell->range.end()) - offset};
                return run;
            }
        }
    }

    /// FETCH: no tier serves `offset`. The extent covers what one source read must pull to serve
    /// `offset` and fill the segment(s) covering it:
    ///  - right: coalesce forward to the nearest offset any tier already holds (so one read fills
    ///    several cells and never re-reads a resident block), capped at `max_fetch_ahead` (the window).
    ///  - left: down to the committed frontier of each populating segment covering `offset` - an
    ///    incremental segment fills append-only from its frontier (its start when virgin), below
    ///    `offset` when the read opens mid-segment; a whole-segment tier's frontier is its cell start.
    ///  - the head whole-segment cell must be fetched WHOLE (populated only by a full-cell write), so
    ///    its end overrides both the coalesce and the window cap.
    size_t fetch_end = span_end;
    for (const auto & tier : tiers)
        fetch_end = std::min(fetch_end, firstServableAtOrAfter(tier, offset, span_end));
    if (max_fetch_ahead < span_end - offset)
        fetch_end = std::min(fetch_end, offset + max_fetch_ahead);

    size_t fetch_start = offset;
    for (const auto & tier : tiers)
    {
        if (!tier.populates)
            continue;
        const CacheResolution * cell = cellCovering(tier, offset);
        if (!cell || cell->kind != CacheResolution::Kind::Miss || !cell->writer)
            continue;
        fetch_start = std::min(fetch_start, cell->writer->committed());
        if (cell->writer->fillsWholeSegment())
            /// The head whole-segment cell is populated only by a full-cell write, so it must be fetched
            /// entire - to its true segment end even past `span_end` (source always has it; cells never
            /// straddle an object boundary). When a slower tier already holds part of the cell this
            /// re-reads it from source: with two populating layers (page + filesystem cache) we aim for
            /// correctness, not for the fewest source bytes.
            fetch_end = std::max(fetch_end, cell->range.end());
    }

    run.range = ByteRange{fetch_start, fetch_end - fetch_start};
    return run;
}

VectorWithMemoryTracking<CacheWriter *> ReadPlan::writersFor(ByteRange range) const
{
    VectorWithMemoryTracking<CacheWriter *> writers;
    for (const auto & tier : tiers)
    {
        if (!tier.populates)
            continue;
        for (const auto & cell : tier.cells)
            if (cell.kind == CacheResolution::Kind::Miss && cell.writer && overlaps(cell.range, range))
                writers.push_back(cell.writer.get());
    }
    return writers;
}

void ReadPlan::extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved)
{
    if (tiers.empty())
    {
        /// First span after `reset`: adopt the tier list (metadata + cells), fastest-first. `span_start`
        /// was set by `reset`; a leading cell may overhang below it (segment rounding), which is fine.
        tiers = std::move(resolved);
    }
    else
    {
        /// Append each provider's new cells to its tier, dropping any that start before the already-held
        /// end (a segment overhanging the previous sub-span is re-returned by the next `resolve`).
        for (size_t i = 0; i < tiers.size() && i < resolved.size(); ++i)
        {
            size_t held_end = tiers[i].cells.empty() ? span_start : tiers[i].cells.back().range.end();
            for (auto & cell : resolved[i].cells)
            {
                if (cell.range.offset < held_end)
                    continue;
                held_end = cell.range.end();
                tiers[i].cells.push_back(std::move(cell));
            }
        }
    }
    span_end = new_end;
}

void ReadPlan::retireBefore(size_t offset)
{
    if (offset <= span_start)
        return;
    for (auto & tier : tiers)
    {
        auto & cells = tier.cells;
        size_t keep = 0;
        while (keep < cells.size() && cells[keep].range.end() <= offset)
            ++keep;
        if (keep > 0)
            cells.erase(cells.begin(), cells.begin() + keep);   /// releases the pinned readers/writers
    }
    span_start = std::min(offset, span_end);
}

void ReadPlan::reset(size_t start_offset)
{
    tiers.clear();
    span_start = start_offset;
    span_end = start_offset;
}

}
