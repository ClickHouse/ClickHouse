#include <IO/ResidencyIterator.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{

ResidencyIterator::ResidencyIterator(
    const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & chain,
    const StoredObject & object_,
    size_t object_file_offset_,
    ByteRange span_)
    : object(object_)
    , object_file_offset(object_file_offset_)
    , probed_span(span_)
{
    for (const auto & provider : chain)
    {
        TierWalk walk;
        walk.provider = provider.get();
        walk.provider->resetProbe();
        walk.view = std::make_unique<CacheView>();
        walk.collected_until = probed_span.offset;
        tiers.push_back(std::move(walk));
    }
}

ChainResolution ResidencyIterator::lookAt(size_t pos)
{
    chassert(pos >= probed_span.offset && pos < probed_span.end());

    ChainResolution res;
    size_t stride_end = probed_span.end();
    for (auto & t : tiers)
    {
        const bool inside_current = t.current_valid
            && t.current.kind != ICacheProvider::Resolution::Kind::End
            && pos >= t.current.range.offset && pos < t.current.range.end();
        if (!inside_current)
        {
            t.current = t.provider->lookAt(object, object_file_offset, pos);
            t.current_valid = true;

            /// Collect forward-new entries into the tier's assembled view (in
            /// offset order); a rewound re-ask resolves ranges only - its
            /// entry is already collected and its reader already handed out.
            /// New territory is keyed on the entry's END: entries are disjoint
            /// and ordered, and a head cell may round BELOW the span start.
            if (t.current.range.end() > t.collected_until)
            {
                if (t.current.kind == ICacheProvider::Resolution::Kind::Hit)
                {
                    /// Store span-clamped, as the batch probe (asked with
                    /// exactly the span) reported hits; the reader may serve
                    /// wider - the executor clamps.
                    const size_t lo = std::max(t.current.range.offset, probed_span.offset);
                    const size_t hi = std::min(t.current.range.end(), probed_span.end());
                    if (lo < hi)
                        t.view->hit_entries.push_back(
                            HitEntry{ByteRange{lo, hi - lo}, std::move(t.current.reader)});
                    t.collected_until = std::max(t.collected_until, t.current.range.end());
                }
                else if (t.current.kind == ICacheProvider::Resolution::Kind::Miss)
                {
                    t.view->miss_entries.push_back(MissEntry{t.current.range, nullptr});
                    t.collected_until = std::max(t.collected_until, t.current.range.end());
                }
            }
        }

        ChainResolution::TierSlice slice;
        switch (t.current.kind)
        {
            case ICacheProvider::Resolution::Kind::Hit:
            {
                slice.state = ChainResolution::TierState::Resident;
                const size_t lo = std::max(t.current.range.offset, probed_span.offset);
                const size_t hi = std::min(t.current.range.end(), probed_span.end());
                slice.extent = ByteRange{lo, hi - lo};
                stride_end = std::min(stride_end, t.current.range.end());
                break;
            }
            case ICacheProvider::Resolution::Kind::Miss:
                slice.state = ChainResolution::TierState::MissCell;
                slice.extent = t.current.range;
                stride_end = std::min(stride_end, t.current.range.end());
                break;
            case ICacheProvider::Resolution::Kind::End:
                break;
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
