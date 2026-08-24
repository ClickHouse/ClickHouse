#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ChainedBuffers.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <limits>

namespace DB
{

/// One tier's cells over the span, in offset order: a hit owns a reader, a miss a writer - except a
/// writer-less miss (bypass / detached / read-only tier), which is served from source, never populated.
struct PlanTier
{
    CacheTier tier{};
    VectorWithMemoryTracking<ICacheProvider::CacheResolution> cells;
};

/// A multi-tier view of a look-ahead span, held across serves: resolve the cache residency once, keep
/// the pinned hit-readers and miss-writers, grow right (`extend`) and retire the consumed prefix left
/// (`retireBefore`) as the cursor advances - so each window serves without re-resolving. It only
/// DESCRIBES residency (`runAt`); the executor does the fetching and download coordination. Not
/// thread-safe; one instance per executor.
class ReadPlan
{
public:
    /// The run serving `[offset, range.end())`, from exactly one source: `from_memory` (executor-local
    /// held bytes, see `readMemory`), a hit `reader`, a committed miss `writer`, or a FETCH (all unset) -
    /// a source read of the extent no tier serves.
    struct PlanRun
    {
        ByteRange range{};
        CacheReader * reader = nullptr;
        CacheWriter * writer = nullptr;
        bool from_memory = false;
        bool isFetch() const { return reader == nullptr && writer == nullptr && !from_memory; }
    };

    bool empty() const { return span_end <= span_start; }
    size_t spanStart() const { return span_start; }
    size_t resolvedEnd() const { return span_end; }
    bool coversForward(size_t offset) const { return offset >= span_start && offset < span_end; }

    /// The run at `offset` (see `PlanRun`); a FETCH extent is capped at `max_fetch_ahead` (the window),
    /// other runs ignore it.
    PlanRun runAt(size_t offset, size_t max_fetch_ahead = std::numeric_limits<size_t>::max()) const;

    /// The populating tiers' writers overlapping `range` - the write-up targets for one FETCH read.
    VectorWithMemoryTracking<CacheWriter *> writersFor(ByteRange range) const;

    /// Resolve `[resolvedEnd(), new_end)` across EVERY provider in `chain` (one `PlanTier` per provider,
    /// fastest-first) and append it - so the plan itself guarantees all layers are asked. `object` and
    /// `object_offset` locate `range` in object space for `resolve`.
    void extend(size_t new_end, const CacheChain & chain,
                const StoredObject & object, size_t object_offset, ByteRange range);

    /// Low-level append of a pre-resolved span, matched to the held tiers by `CacheTier`. Used by the
    /// chain-driving `extend` above and by unit tests that inject residency directly.
    void extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved);

    /// The executor-local memory hold - fetched bytes no tier accepted (read-only / detached / rejected
    /// write). `hold` keeps them (served by a `from_memory` run), `readMemory` serves a sub-range; freed
    /// as `retireBefore` passes them, so already-fetched bytes are never re-read.
    void hold(ChainedBuffers bytes);
    ChainedBuffers readMemory(ByteRange range) const;

    /// Drop cells that end at or before `offset` (release their pins) and advance `spanStart` to it.
    void retireBefore(size_t offset);

    /// Discard everything and re-anchor the (empty) span at `start_offset` - a seek, a backward jump,
    /// or the first build. The next `extend` grows forward from here.
    void reset(size_t start_offset);

private:
    size_t span_start = 0;
    size_t span_end = 0;   /// `[span_start, span_end)` is resolved
    VectorWithMemoryTracking<PlanTier> tiers;   /// fastest-first, 1:1 with the cache chain
    ChainedBuffers memory;   /// fetched bytes no tier accepted; served first, freed on retire
};

}
