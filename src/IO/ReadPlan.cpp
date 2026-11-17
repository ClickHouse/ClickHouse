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
/// `range_end` - where a FETCH must stop so it never overruns bytes a tier already holds.
size_t firstServableAtOrAfter(const PlanTier & tier, size_t from, size_t range_end)
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
    return range_end;
}

}

ReadPlan::PlanRun ReadPlan::runAt(size_t offset, size_t fetch_limit) const
{
    if (offset < range_start || offset >= range_end)
        return std::monostate{};

    /// memory hold - serve to its first gap
    if (memory.contains(offset))
    {
        size_t end = memory.range().end();
        if (auto g = memory.gaps(ByteRange{offset, end - offset}); !g.empty())
            end = g.front().offset;
        return ServeFromMemory{ByteRange{offset, end - offset}, &memory};
    }

    /// Read each covering writer's `committed` once: a concurrent downloader moves it, and a second read disagreeing
    /// with the serve check would size an empty fetch. Hence the servable scan starts after the covering cell.
    size_t fetch_end = range_end;
    size_t fetch_start = offset;
    for (const auto & tier : tiers)
    {
        size_t scan_from = offset;
        if (const CacheResolution * cell = cellCovering(tier, offset))
        {
            if (cell->kind == CacheResolution::Kind::Hit && cell->reader)
                return ServeFromReader{ByteRange{offset, cell->range.end() - offset}, cell->reader.get()};
            if (cell->kind == CacheResolution::Kind::Miss && cell->writer)
            {
                const size_t committed = cell->writer->committed();
                if (offset < committed)
                    return ServeFromWriter{ByteRange{offset, committed - offset}, cell->writer.get()};
                fetch_start = std::min(fetch_start, committed);
            }
            scan_from = cell->range.end();
        }
        fetch_end = std::min(fetch_end, firstServableAtOrAfter(tier, scan_from, range_end));
    }
    if (fetch_limit < range_end - offset)
        fetch_end = std::min(fetch_end, offset + fetch_limit);

    /// Widen to complete every whole-segment cell the fetch enters (fixpoint).
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

    return Fetch{ByteRange{fetch_start, fetch_end - fetch_start}};
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

void ReadPlan::extend(size_t new_end, const CacheChain & chain,
                      const StoredObject & object, size_t object_offset, ByteRange range)
{
    /// Ask EVERY layer, fastest-first, so no tier is ever skipped - the plan owns this invariant.
    VectorWithMemoryTracking<PlanTier> resolved;
    for (const auto & cache : chain)
    {
        PlanTier pt;
        pt.tier = cache->tier();
        pt.cells = cache->resolve(object, object_offset, range);
        resolved.push_back(std::move(pt));
    }
    extend(new_end, std::move(resolved));
}

void ReadPlan::extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved)
{
    if (tiers.empty())
    {
        tiers = std::move(resolved);   /// first range after `reset`: adopt the tier list, fastest-first
    }
    else
    {
        /// Pair by chain position: both vectors come from one ordered walk of the same chain. `CacheTier`
        /// does NOT identify a tier - a stacked cache-on-cache chain repeats it, and matching on it would
        /// fold one entry into two held tiers, leaving the second with moved-from (null) cells.
        chassert(resolved.size() == tiers.size());
        for (size_t i = 0; i < tiers.size(); ++i)
        {
            auto & held = tiers[i];
            chassert(resolved[i].tier == held.tier);
            size_t held_end = held.cells.empty() ? range_start : held.cells.back().range.end();
            for (auto & cell : resolved[i].cells)
            {
                if (cell.range.end() <= held_end)
                    continue;
                if (cell.range.offset < held_end)
                    cell.range = ByteRange{held_end, cell.range.end() - held_end};
                held_end = cell.range.end();
                held.cells.push_back(std::move(cell));
            }
        }
    }
    range_end = new_end;
}

void ReadPlan::dropCellsOutsideRange()
{
    const ByteRange kept{range_start, range_end > range_start ? range_end - range_start : 0};
    for (auto & tier : tiers)
        std::erase_if(tier.cells, [&](const CacheResolution & c) { return !c.range.overlaps(kept); });
}

void ReadPlan::dropBefore(size_t offset)
{
    if (offset <= range_start)
        return;
    range_start = std::min(offset, range_end);
    dropCellsOutsideRange();
    if (!memory.empty())
    {
        const size_t mend = memory.range().end();
        memory = range_start < mend ? memory.slice(ByteRange{range_start, mend - range_start}) : ChainedBuffers{};
    }
}

void ReadPlan::dropAfter(size_t offset)
{
    if (offset >= range_end)
        return;
    range_end = std::max(offset, range_start);
    dropCellsOutsideRange();
    if (!memory.empty())
    {
        const size_t mstart = memory.range().offset;
        memory = range_end > mstart ? memory.slice(ByteRange{mstart, range_end - mstart}) : ChainedBuffers{};
    }
}

void ReadPlan::reset(size_t start_offset)
{
    tiers.clear();
    memory = {};
    range_start = start_offset;
    range_end = start_offset;
}

}
