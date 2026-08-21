#include <IO/ReadPlan.h>

#include <algorithm>
#include <limits>
#include <optional>

namespace DB
{

using CacheResolution = ICacheProvider::CacheResolution;

namespace
{

constexpr size_t NPOS = std::numeric_limits<size_t>::max();

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

/// End of the committed run containing `off` (0-length-safe), or nullopt when `off` is not committed.
std::optional<size_t> committedRunEnd(const IntervalSet & committed, size_t off)
{
    for (const auto & iv : committed.ranges())
        if (iv.offset <= off && off < iv.end())
            return iv.end();
    return std::nullopt;
}

/// The smallest offset >= `from` this tier can serve (a hit run, or a committed sub-range of a miss
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
        if (cell.kind == CacheResolution::Kind::Miss && cell.writer)
        {
            size_t best = NPOS;
            for (const auto & iv : cell.writer->committed().ranges())
                if (iv.end() > base)
                    best = std::min(best, std::max(iv.offset, base));
            if (best != NPOS)
                return best;
        }
    }
    return span_end;
}

}

ReadPlan::PlanRun ReadPlan::runAt(size_t offset) const
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
            if (auto end = committedRunEnd(cell->writer->committed(), offset))
            {
                run.writer = cell->writer.get();
                run.range = ByteRange{offset, std::min(*end, cell->range.end()) - offset};
                return run;
            }
        }
    }

    /// FETCH: no tier serves `offset`. Coalesce forward to the nearest offset any tier serves, so one
    /// source read fills the whole run across cells and tiers without re-reading a resident block.
    size_t fetch_end = span_end;
    for (const auto & tier : tiers)
        fetch_end = std::min(fetch_end, firstServableAtOrAfter(tier, offset, span_end));
    run.range = ByteRange{offset, fetch_end - offset};
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
