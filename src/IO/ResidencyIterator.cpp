#include <IO/ResidencyIterator.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{

ResidencyIterator::ResidencyIterator(
    const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & chain,
    const StoredObject & object,
    size_t object_file_offset,
    ByteRange span_)
    : probed_span(span_)
    , last_pos(span_.offset)
{
    for (const auto & provider : chain)
    {
        TierWalk walk;
        walk.view = provider->planResidencyView(object, object_file_offset, probed_span);

        /// Merge the view's sorted hit and miss lists into one classified walk.
        /// Hits are clamped to the span on both edges, as the batch fold records
        /// them; a miss keeps its whole cell as the reported extent while only
        /// its in-span portion drives stride boundaries.
        const auto & hits = walk.view->hits();
        const auto & misses = walk.view->misses();
        size_t hi = 0;
        size_t mi = 0;
        while (hi < hits.size() || mi < misses.size())
        {
            const bool take_hit = mi >= misses.size()
                || (hi < hits.size() && hits[hi].range.offset < misses[mi].range.offset);
            const ByteRange raw = take_hit ? hits[hi].range : misses[mi].range;
            const size_t lo = std::max(raw.offset, probed_span.offset);
            const size_t up = std::min(raw.end(), probed_span.end());
            if (lo < up)
            {
                Classified c;
                c.stride = ByteRange{lo, up - lo};
                c.extent = take_hit ? c.stride : raw;
                c.state = take_hit ? ChainResolution::TierState::Resident
                                   : ChainResolution::TierState::MissCell;
                walk.entries.push_back(c);
            }
            take_hit ? ++hi : ++mi;
        }

        tiers.push_back(std::move(walk));
    }
}

ChainResolution ResidencyIterator::lookAt(size_t pos)
{
    chassert(pos >= probed_span.offset && pos < probed_span.end());

    if (pos < last_pos)
        for (auto & t : tiers)
            t.cursor = 0;
    last_pos = pos;

    ChainResolution res;
    size_t stride_end = probed_span.end();
    for (auto & t : tiers)
    {
        while (t.cursor < t.entries.size() && t.entries[t.cursor].stride.end() <= pos)
            ++t.cursor;

        ChainResolution::TierSlice slice;
        if (t.cursor < t.entries.size() && t.entries[t.cursor].stride.offset <= pos)
        {
            const auto & e = t.entries[t.cursor];
            slice.state = e.state;
            slice.extent = e.extent;
            stride_end = std::min(stride_end, e.stride.end());
        }
        else
        {
            const size_t next_start = t.cursor < t.entries.size()
                ? t.entries[t.cursor].stride.offset
                : probed_span.end();
            stride_end = std::min(stride_end, next_start);
        }
        res.tiers.push_back(slice);
    }

    chassert(stride_end > pos);
    res.range = ByteRange{pos, stride_end - pos};
    return res;
}

ResolutionFold::ResolutionFold(VectorWithMemoryTracking<TierTraits> traits_, ByteRange span_)
    : traits(std::move(traits_))
    , span(span_)
{
    accs.resize(traits.size());
}

void ResolutionFold::add(const ChainResolution & r)
{
    chassert(r.tiers.size() == traits.size());

    for (size_t i = 0; i < traits.size(); ++i)
    {
        const auto & slice = r.tiers[i];
        auto & acc = accs[i];

        if (slice.state == ChainResolution::TierState::Resident)
        {
            /// One record per hit entry: consecutive strides over the same run
            /// repeat the same extent.
            if (acc.resident.empty() || acc.resident.back().offset != slice.extent.offset
                || acc.resident.back().size != slice.extent.size)
                acc.resident.push_back(slice.extent);
            flushCell(acc);
            continue;
        }

        if (slice.state != ChainResolution::TierState::MissCell || !traits[i].populates)
        {
            flushCell(acc);
            continue;
        }

        if (!acc.pending_cell || acc.pending_cell->offset != slice.extent.offset
            || acc.pending_cell->size != slice.extent.size)
        {
            flushCell(acc);
            acc.pending_cell = slice.extent;
            /// A cell overhanging the span can never be fully covered by the
            /// span-clamped faster hits, so it always survives the prune.
            acc.pending_uncovered
                = slice.extent.offset < span.offset || slice.extent.end() > span.end();
        }

        bool covered_here = false;
        for (size_t j = 0; j < i && !covered_here; ++j)
            covered_here = r.tiers[j].state == ChainResolution::TierState::Resident;
        acc.pending_uncovered = acc.pending_uncovered || !covered_here;
    }
}

VectorWithMemoryTracking<GeometryEntry> ResolutionFold::finish()
{
    VectorWithMemoryTracking<GeometryEntry> entries;
    for (size_t i = 0; i < traits.size(); ++i)
    {
        auto & acc = accs[i];
        flushCell(acc);
        if (acc.resident.empty() && acc.cells.empty())
            continue;

        GeometryEntry entry;
        entry.tier = traits[i].tier;
        entry.whole_cell = traits[i].whole_cell;
        entry.resident = std::move(acc.resident);
        entry.aligned_miss = std::move(acc.cells);
        entries.push_back(std::move(entry));
    }
    return entries;
}

void ResolutionFold::flushCell(TierAcc & acc)
{
    if (acc.pending_cell && acc.pending_uncovered)
        acc.cells.push_back(*acc.pending_cell);
    acc.pending_cell.reset();
    acc.pending_uncovered = false;
}

}
