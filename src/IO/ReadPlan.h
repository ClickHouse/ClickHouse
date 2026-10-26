#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ChainedBuffers.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <variant>

namespace DB
{

/// One tier's cells over the range, in offset order: a hit owns a reader, a miss a writer - except a
/// writer-less miss (bypass / detached / read-only tier), which is served from source, never populated.
struct PlanTier
{
    CacheTier tier{};
    VectorWithMemoryTracking<ICacheProvider::CacheResolution> cells;
};

/// A multi-tier view of a look-ahead range, held across serves: resolve the cache residency once, keep
/// the pinned hit-readers and miss-writers, grow right (`extend`) and drop the consumed prefix left
/// (`dropBefore`) as the cursor advances - so each window serves without re-resolving. It only
/// DESCRIBES residency (`runAt`); the executor does the fetching and download coordination. Not
/// thread-safe; one instance per executor.
class ReadPlan
{
public:
    /// The outcome of `runAt`: how to serve `[offset, range.end())`. Exactly one alternative, each
    /// carrying only what that source needs. `std::monostate` means `offset` is outside the resolved range.
    struct ServeFromReader { ByteRange range; CacheReader * reader = nullptr; };          /// an already-cached hit
    struct ServeFromWriter { ByteRange range; CacheWriter * writer = nullptr; };          /// a committed miss prefix
    struct ServeFromMemory { ByteRange range; const ChainedBuffers * memory = nullptr; }; /// the plan's memory hold
    struct Fetch { ByteRange range; };                                                    /// source-read, then fill
    using PlanRun = std::variant<std::monostate, ServeFromReader, ServeFromWriter, ServeFromMemory, Fetch>;

    bool empty() const { return range_end <= range_start; }
    size_t begin() const { return range_start; }
    size_t end() const { return range_end; }
    bool contains(size_t offset) const { return offset >= range_start && offset < range_end; }

    /// How to serve `offset` (see `PlanRun`): the memory hold, the fastest tier covering it, else a
    /// `Fetch` from source, whose range may reach past the asked one on either side.
    PlanRun runAt(size_t offset, size_t fetch_limit) const;

    /// The populating tiers' writers overlapping `range` - the write-up targets for one FETCH read.
    VectorWithMemoryTracking<CacheWriter *> writersFor(ByteRange range) const;

    /// Resolve `[end(), new_end)` across EVERY provider in `chain` (one `PlanTier` per provider,
    /// fastest-first) and append it - so the plan itself guarantees all layers are asked. `object` and
    /// `object_offset` locate `range` in object space for `resolve`.
    void extend(size_t new_end, const CacheChain & chain,
                const StoredObject & object, size_t object_offset, ByteRange range);

    /// Low-level append of a pre-resolved range, matched to the held tiers by `CacheTier`. Used by the
    /// chain-driving `extend` above and by unit tests that inject residency directly.
    void extend(size_t new_end, VectorWithMemoryTracking<PlanTier> resolved);

    /// The executor-local memory hold - fetched bytes no tier accepted (read-only / detached / rejected
    /// write). `hold` keeps them (served by a `ServeFromMemory` run that points at this hold); freed as
    /// `dropBefore` passes them, so already-fetched bytes are never re-read.
    void hold(ChainedBuffers bytes);

    /// Move `begin` forward to `offset`, releasing what it leaves behind.
    void dropBefore(size_t offset);

    /// Move `end` back to `offset`, releasing what lies past it - for when the look-ahead shrinks.
    void dropAfter(size_t offset);

    /// Discard everything and re-anchor the (empty) range at `start_offset` - a seek, a backward jump,
    /// or the first build. The next `extend` grows forward from here.
    void reset(size_t start_offset);

private:
    void dropCellsOutsideRange();

    size_t range_start = 0;
    size_t range_end = 0;   /// `[range_start, range_end)` is resolved
    VectorWithMemoryTracking<PlanTier> tiers;   /// fastest-first, 1:1 with the cache chain
    ChainedBuffers memory;   /// fetched bytes no tier accepted; served first, freed on drop
};

}
