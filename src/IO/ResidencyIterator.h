#pragma once

#include <IO/CoverageMap.h>
#include <IO/ICacheProvider.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

namespace DB
{

/// Chain-level residency resolution at a position: one stride over which every
/// tier's classification is constant, with the per-tier column (fastest-first,
/// positional with the chain the iterator was built over). A `Resident` slice
/// carries the tier's hit run clamped to the probed span; a `MissCell` slice
/// carries the tier's whole cell (object-end-clamped only, so it may overhang
/// the span). `Absent` marks the EOF tail where the tier's tiling ends.
struct ChainResolution
{
    static constexpr size_t npos = static_cast<size_t>(-1);

    enum class TierState : uint8_t
    {
        Absent,
        Resident,
        MissCell,
    };

    struct TierSlice
    {
        TierState state = TierState::Absent;
        ByteRange extent{};
    };

    ByteRange range{};
    VectorWithMemoryTracking<TierSlice> tiers;

    /// The fastest tier resident over the stride; `npos` = no tier holds it.
    size_t hitTier() const
    {
        for (size_t i = 0; i < tiers.size(); ++i)
            if (tiers[i].state == TierState::Resident)
                return i;
        return npos;
    }

    bool hit() const { return hitTier() != npos; }
};

/// Walks one object-piece's residency across the cache chain one resolution at
/// a time: `lookAt(pos)` resolves a position into its chain column and the
/// stride the column stays constant over, so a caller covers a span by
/// iterating `pos = lookAt(pos).range.end()`. Each tier is stepped through the
/// provider's `lookAt` (memoized chunked probing underneath - lazy pinning),
/// re-asked only at its own boundaries; the handed-out hit readers and the
/// walked miss cells assemble into the per-tier view the plan takes over.
class ResidencyIterator
{
public:
    ResidencyIterator(
        const VectorWithMemoryTracking<std::shared_ptr<ICacheProvider>> & chain,
        const StoredObject & object_,
        size_t object_file_offset_,
        ByteRange span_);

    ChainResolution lookAt(size_t pos);

    ByteRange span() const { return probed_span; }

    /// Hand over the tier's assembled view - the hit readers (pinning their
    /// segments) and the walked miss cells - to become the plan's held
    /// buffers. `lookAt` keeps working (re-asks return null readers), but the
    /// readers travel with the view.
    CacheViewPtr takeView(size_t tier_idx)
    {
        chassert(tier_idx < tiers.size());
        return std::move(tiers[tier_idx].view);
    }

private:
    struct TierWalk
    {
        ICacheProvider * provider = nullptr;
        /// The walk's own probe state; its pins die with the iterator - what
        /// the plan keeps pinned travels in the handed-out readers.
        std::unique_ptr<ICacheProvider::IProbeCursor> probe;
        /// The tier's current resolution, reused while the asked position
        /// stays inside it.
        ICacheProvider::Resolution current;
        bool current_valid = false;
        /// Entries collected from the walked resolutions, in offset order -
        /// the plan's held view. `collected_until` keeps a rewound re-ask
        /// from collecting an entry twice.
        CacheViewPtr view;
        size_t collected_until = 0;
    };

    StoredObject object;
    size_t object_file_offset;
    ByteRange probed_span;
    VectorWithMemoryTracking<TierWalk> tiers;
};

/// Accumulates a forward walk of resolutions over a span back into the batch
/// observation's per-tier geometry: resident runs recorded per hit entry, miss
/// cells kept only when NOT fully covered by faster tiers' hits (the
/// `upper_hits` prune), bypass tiers contributing no cells, tiers with nothing
/// resident and nothing to fill dropped. The equivalence gate proving the
/// iterated observation reproduces `observeAndSchedule`'s flat fold.
/// Assemble a span-probe-shaped view (sorted hit entries with readers + one-cell
/// miss entries) by walking `lookAt` over `span` - for callers that need one
/// whole small range at once: the encryption-header read, the buffer-API tests.
CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span);

class ResolutionFold
{
public:
    struct TierTraits
    {
        CacheTier tier{};
        bool whole_cell = false;
        bool populates = true;
    };

    ResolutionFold(VectorWithMemoryTracking<TierTraits> traits_, ByteRange span_);

    void add(const ChainResolution & r);

    /// One entry per tier, positional with the traits (an entry with nothing
    /// resident and nothing to fill stays empty - the caller drops or skips
    /// it, as the batch fold drops empty views).
    VectorWithMemoryTracking<GeometryEntry> finish();

private:
    struct TierAcc
    {
        VectorWithMemoryTracking<ByteRange> resident;
        VectorWithMemoryTracking<ByteRange> cells;
        std::optional<ByteRange> pending_cell;
        bool pending_uncovered = false;
    };

    void flushCell(TierAcc & acc);

    VectorWithMemoryTracking<TierTraits> traits;
    ByteRange span;
    VectorWithMemoryTracking<TierAcc> accs;
};

}
