#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ResidencyIterator.h>

namespace DB::tests
{

/// Adapts a span-probe MOCK to the cache-provider `resolve` API: the mock keeps
/// its whole residency logic in `buildProbeView` (sorted disjoint hit/miss
/// entries tiling the ranged ask, one cell per miss) and this base runs it ONCE
/// per `resolve` and hands the entries out as offset-ordered resolutions - a hit
/// carries its reader, a miss its writer (both moved out of the view). Test-only;
/// the real providers implement `resolve` natively.
class SpanProbeMockBase : public ICacheProvider
{
public:
    VectorWithMemoryTracking<CacheResolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) override
    {
        auto view = buildProbeView(object, object_file_offset, range);
        auto & hits = view->hit_entries;
        auto & misses = view->miss_entries;

        VectorWithMemoryTracking<CacheResolution> out;
        out.reserve(hits.size() + misses.size());
        size_t hi = 0;
        size_t mi = 0;
        /// Merge the two sorted disjoint lists into one offset-ordered stream.
        while (hi < hits.size() || mi < misses.size())
        {
            const bool take_hit = mi >= misses.size()
                || (hi < hits.size() && hits[hi].range.offset <= misses[mi].range.offset);
            if (take_hit)
            {
                out.push_back(CacheResolution{CacheResolution::Kind::Hit, hits[hi].range, std::move(hits[hi].reader), nullptr});
                ++hi;
            }
            else
            {
                out.push_back(CacheResolution{CacheResolution::Kind::Miss, misses[mi].range, nullptr, std::move(misses[mi].writer)});
                ++mi;
            }
        }
        return out;
    }

protected:
    /// The mock's residency logic, in the span-probe shape: sorted disjoint
    /// hit/miss entries tiling the (object-clamped) request, one cell per miss.
    virtual CacheViewPtr buildProbeView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) = 0;
};

}
