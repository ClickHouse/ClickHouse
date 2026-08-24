#include <IO/ReadPlan.h>

#include <algorithm>

namespace DB
{

using CacheResolution = ICacheProvider::CacheResolution;

namespace
{

/// The cell of `tier` whose range contains `off`, or null when `off` is past the tier's cells.
const CacheResolution * cellCovering(const PlanTier & tier, size_t off)
{
    for (const auto & cell : tier.cells)
        if (cell.range.offset <= off && off < cell.range.end())
            return &cell;
    return nullptr;
}

/// The smallest offset >= `from` this tier can serve (a hit, or a miss's committed prefix), else
/// `span_end` - where a FETCH must stop so it never overruns bytes a tier already holds.
size_t firstServableAtOrAfter(const PlanTier & tier, size_t from, size_t span_end)
{
    for (const auto & cell : tier.cells)
    {
        if (cell.range.end() <= from)
            continue;
        const size_t base = std::max(cell.range.offset, from);
        if (cell.kind == CacheResolution::Kind::Hit && cell.reader)
            return base;
        if (cell.kind == CacheResolution::Kind::Miss && cell.writer && cell.writer->committed() > base)
            return base;   /// `base` is inside the committed prefix `[cell.offset, committed())`
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

    /// Memory hold (already fetched, no tier took it) is fastest - serve to its first gap.
    if (!memory.empty() && memory.covers(ByteRange{offset, 1}))
    {
        size_t end = memory.range().end();
        if (auto g = memory.gaps(ByteRange{offset, end - offset}); !g.empty())
            end = g.front().offset;
        run.from_memory = true;
        run.range = ByteRange{offset, end - offset};
        return run;
    }

    /// Fastest tier serving `offset`: a hit reader, or a miss committed here (served from its writer).
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
            const size_t committed = cell->writer->committed();
            if (offset < committed)
            {
                run.writer = cell->writer.get();
                run.range = ByteRange{offset, std::min(committed, cell->range.end()) - offset};
                return run;
            }
        }
    }

    /// FETCH: the single source read that serves `offset` and fills the covering segments.
    /// Right: coalesce to the nearest resident offset (one read fills several cells, never re-reads a
    /// resident block), capped at the window.
    size_t fetch_end = span_end;
    for (const auto & tier : tiers)
        fetch_end = std::min(fetch_end, firstServableAtOrAfter(tier, offset, span_end));
    if (max_fetch_ahead < span_end - offset)
        fetch_end = std::min(fetch_end, offset + max_fetch_ahead);

    /// Left: down to the committed frontier of each populating segment covering `offset` - below
    /// `offset` when the read opens mid-segment (an incremental segment fills from its frontier).
    size_t fetch_start = offset;
    for (const auto & tier : tiers)
    {
        const CacheResolution * cell = cellCovering(tier, offset);
        if (cell && cell->kind == CacheResolution::Kind::Miss && cell->writer)
            fetch_start = std::min(fetch_start, cell->writer->committed());
    }

    /// A whole-segment cell is populated only whole, so cover EVERY one the fetch enters (else a cell it
    /// stops inside gets a rejected partial write and is re-read later). Fixpoint - converges on a
    /// boundary; its end may pass `span_end` (source has it; cells never cross an object).
    for (bool grew = true; grew;)
    {
        grew = false;
        for (const auto & tier : tiers)
        {
            for (const auto & cell : tier.cells)
                if (cell.kind == CacheResolution::Kind::Miss && cell.writer && cell.writer->fillsWholeSegment()
                    && cell.range.offset < fetch_end && cell.range.end() > fetch_end)
                {
                    fetch_end = cell.range.end();
                    grew = true;
                }
        }
    }

    run.range = ByteRange{fetch_start, fetch_end - fetch_start};
    return run;
}

VectorWithMemoryTracking<CacheWriter *> ReadPlan::writersFor(ByteRange range) const
{
    VectorWithMemoryTracking<CacheWriter *> writers;
    for (const auto & tier : tiers)
    {
        for (const auto & cell : tier.cells)
            if (cell.kind == CacheResolution::Kind::Miss && cell.writer && cell.range.overlaps(range))
                writers.push_back(cell.writer.get());
    }
    return writers;
}

void ReadPlan::hold(ChainedBuffers bytes)
{
    memory.append(std::move(bytes));
}

ChainedBuffers ReadPlan::readMemory(ByteRange range) const
{
    return memory.slice(range);
}

void ReadPlan::extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved)
{
    if (tiers.empty())
    {
        tiers = std::move(resolved);   /// first span after `reset`: adopt the tier list, fastest-first
    }
    else
    {
        /// Append each tier's new cells, dropping any starting before its held end (a segment
        /// overhanging the previous sub-span is re-returned by the next `resolve`).
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
    /// Free the memory hold the cursor has passed; keep what is still ahead.
    if (!memory.empty())
    {
        const size_t mend = memory.range().end();
        memory = offset < mend ? memory.slice(ByteRange{offset, mend - offset}) : ChainedBuffers{};
    }
    span_start = std::min(offset, span_end);
}

void ReadPlan::reset(size_t start_offset)
{
    tiers.clear();
    memory = {};
    span_start = start_offset;
    span_end = start_offset;
}

}
