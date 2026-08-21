#pragma once

#include <IO/ICacheProvider.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

namespace DB
{

/// One cache tier's resolution over the plan span, held across serves: the tier's cells in offset
/// order (a hit owns a reader, a populating miss owns a writer), tiling `[ReadPlan::spanStart,
/// ReadPlan::resolvedEnd)`. `populates` mirrors `ICacheProvider::populatesOnMiss` - a bypass tier
/// contributes hits but never write targets.
struct PlanTier
{
    CacheTier tier{};
    bool populates = false;
    VectorWithMemoryTracking<ICacheProvider::CacheResolution> cells;
};

/// A resolved multi-tier view of a look-ahead span that PERSISTS across serves. The executor resolves
/// the cache residency once, holds the pinned hit-readers and miss-writers here, grows the span on the
/// right (`extend`) and drops the consumed prefix on the left (`retireBefore`) as the cursor advances -
/// so a read serves one block per window without re-`resolve`-ing every window and reuses the same
/// pins across serves.
///
/// It only DESCRIBES residency: `at` says which tier serves an offset and how far. It never fetches or
/// coordinates downloads - on a miss the executor fetches from source and populates `writersFor`, so
/// the claim / concurrent-download logic stays in one place. Not thread-safe; one instance per
/// executor. (The later prefetch slice fills the held writers ahead of the cursor - each writer is
/// individually locked - without changing this interface.)
class ReadPlan
{
public:
    /// The merged, fastest-tier-wins run covering `offset` - the "universal plan" query. A CACHE run
    /// (`reader` xor `writer` set) is served from that tier for `[offset, range.end())`: a hit, or a
    /// miss segment already committed (served from its writer). A FETCH run (both null) is the
    /// contiguous extent no tier serves, coalesced across cells and tiers up to the nearest offset a
    /// tier holds - so one source read fills several segments and never overruns a resident block.
    struct PlanRun
    {
        ByteRange range{};
        CacheReader * reader = nullptr;
        CacheWriter * writer = nullptr;
        bool isFetch() const { return reader == nullptr && writer == nullptr; }
    };

    bool empty() const { return span_end <= span_start; }
    size_t spanStart() const { return span_start; }
    size_t resolvedEnd() const { return span_end; }
    bool coversForward(size_t offset) const { return offset >= span_start && offset < span_end; }

    PlanRun runAt(size_t offset) const;

    /// The populating tiers' writers overlapping `range` - the write-up targets for one source read
    /// of a FETCH run (several cells across tiers filled from one read).
    VectorWithMemoryTracking<CacheWriter *> writersFor(ByteRange range) const;

    /// Append the resolution of `[resolvedEnd(), new_end)`. `resolved` is one `PlanTier` per provider,
    /// fastest-first, matching the existing tier order (and set on the first call).
    void extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved);

    /// Drop cells that end at or before `offset` (release their pins) and advance `spanStart` to it.
    void retireBefore(size_t offset);

    /// Discard everything and re-anchor the (empty) span at `start_offset` - a seek, a backward jump,
    /// or the first build. The next `extend` grows forward from here.
    void reset(size_t start_offset);

private:
    size_t span_start = 0;
    size_t span_end = 0;   /// `[span_start, span_end)` is resolved
    VectorWithMemoryTracking<PlanTier> tiers;   /// fastest-first, 1:1 with the cache chain
};

}
