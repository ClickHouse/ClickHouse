#pragma once

#include <IO/ChainedBuffers.h>
#include <IO/IntervalSet.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// Which storage tier a cache provider represents; drives per-tier byte
/// attribution in observability.
enum class CacheTier
{
    PageCache,
    FilesystemCache,
};

/// Per-range buffer API: `planResidencyView` + `openWriteBuffers` decompose a
/// request into HIT ranges (each owning a held `CacheReader`) and MISS ranges
/// (each owning a held `CacheWriter`). The buffers are held by the executor's
/// plan across many read windows. Coordinates are FILE-LEVEL throughout.

/// Held, re-readable view of ONE resident (hit) file-level range. Owns the pin
/// that keeps its bytes alive; holds NO cursor (the executor's ChainedBuffers owns it).
class CacheReader
{
public:
    virtual ~CacheReader() = default;

    /// Cache-aligned range this buffer can serve (may be wider than the hit
    /// the plan asked for - the executor clamps).
    virtual ByteRange range() const = 0;

    /// Committed-prefix end. == `range().end()` for a fully-resident segment
    /// or block; for a partially-downloaded disk segment the LIVE write
    /// offset, re-evaluated each call. Reads must stay below it.
    virtual size_t readable() const = 0;

    /// Read `sub` (within `[range().offset, readable())`) as a ChainedBuffers of
    /// file-level nodes. Records `sub` for the view's deferred LRU bump.
    virtual ChainedBuffers read(ByteRange sub) = 0;
};

/// Held, incrementally-fillable target for ONE miss file-level range. Owns its
/// own writable segment ref(s), so it appends across many windows and is
/// finalized only at destruction.
class CacheWriter
{
public:
    virtual ~CacheWriter() = default;

    /// Cache-ALIGNED range; may extend beyond the requested miss range.
    virtual ByteRange range() const = 0;

    /// Bytes within `range()` already committed by this buffer (any order).
    /// Returned BY VALUE: a writer may be written concurrently (the prefetch worker
    /// writes its led segments while the foreground touches the same writer object),
    /// so the writer hands back a snapshot taken under its own lock.
    virtual IntervalSet committed() const = 0;

    bool complete() const { return committed().subtract(range()).empty(); }

    /// Store the portion of `data` within `range()` minus `committed()`.
    /// Returns the bytes that newly landed; 0 for bytes outside the range,
    /// already committed, an unclaimed cell, reservation failure or
    /// bypass - NEVER throws on those, degrades to a partial or zero return.
    /// On tiers that coordinate downloaders (the disk cache) a write lands only
    /// into cells covered by an open `claim` of the calling thread.
    virtual size_t write(ChainedBuffers data) = 0;

    /// Serve an already-committed sub-range from this buffer's own held
    /// segments/cells, without a source round-trip.
    virtual ChainedBuffers read(ByteRange sub) = 0;

    /// One sibling-led sub-range to serve from cache (the writer that owns it + the sub-range).


    /// The result of `claim`: the downloader roles the calling thread holds over one window,
    /// plus the window's decomposition into runs to fetch (`to_fetch` - roles won, or
    /// uncoordinated bytes) and runs a sibling leads (`sibling_led` - already cached, or
    /// being downloaded by another reader; serve them from cache after). Move-only RAII:
    /// roles are thread-affine (the downloader id is the caller id), so the token must be
    /// created, written through, and destroyed on ONE thread; the destructor
    /// completes-and-releases exactly the roles this claim NEWLY won - a nested claim over
    /// cells the thread already leads (a tile write inside a window-long claim) releases
    /// nothing of the outer claim's. Exceptions in the release are swallowed and logged.
    class FillClaim
    {
    public:
        FillClaim() = default;
        FillClaim(FillClaim && other) noexcept
            : to_fetch(std::move(other.to_fetch))
            , sibling_led(std::move(other.sibling_led))
            , release(std::exchange(other.release, nullptr))
        {
        }
        FillClaim & operator=(FillClaim && other) noexcept
        {
            if (this != &other)
            {
                reset();
                to_fetch = std::move(other.to_fetch);
                sibling_led = std::move(other.sibling_led);
                release = std::exchange(other.release, nullptr);
            }
            return *this;
        }
        FillClaim(const FillClaim &) = delete;
        FillClaim & operator=(const FillClaim &) = delete;
        ~FillClaim() { reset(); }

        void reset() noexcept
        {
            if (auto r = std::exchange(release, nullptr))
                r();
        }

        VectorWithMemoryTracking<ByteRange> to_fetch;
        VectorWithMemoryTracking<ByteRange> sibling_led;
        /// Completes-and-releases the newly-won roles; noexcept by construction (the
        /// provider wraps its body in try/catch). Empty when nothing was won.
        std::function<void()> release;
    };

    /// Acquire the downloader roles for the cache cells overlapping `window` (clamped
    /// internally). The ONLY role-acquisition site: `write` never adopts a role, even one a
    /// sibling freed mid-window - a freed cell is picked up by the NEXT claim, which keeps
    /// "which roles does this thread hold" answerable from the live claims alone. Does NOT
    /// wait. Default: the whole window is to-fetch, nothing to release (no coordination,
    /// e.g. page cache).
    virtual FillClaim claim(ByteRange window)
    {
        FillClaim c;
        c.to_fetch.push_back(window);
        return c;
    }

    /// Wait until `sub`'s bytes are committed by the sibling downloader, then serve them
    /// from this writer's own held segments (cache file). Default: plain read (no wait).
    virtual ChainedBuffers waitAndReadSiblingLed(ByteRange sub) { return read(sub); }

    /// Opaque token keeping the partial segment under `frontier`
    /// non-evictable while the live source connection streams into it.
    /// Default no-op (e.g. page cache).
    using CacheSegmentPin = std::shared_ptr<void>;
    virtual CacheSegmentPin pin(size_t /*frontier*/) const { return nullptr; }
};

using CacheReaderPtr = std::unique_ptr<CacheReader>;
using CacheWriterPtr = std::unique_ptr<CacheWriter>;

/// One resident range + its held read buffer.
struct HitEntry { ByteRange range; CacheReaderPtr reader; };
/// One miss CELL. The writer carries the entry's lifecycle: null as probed
/// (`planResidencyView` observes only), opened by `openWriteBuffers` for the
/// misses that survive the plan's prune (null on a read-only/bypass tier).
struct MissEntry { ByteRange range; CacheWriterPtr writer; };

/// Decomposed lookup result, held by the plan across windows - the tier's
/// plan-state object: hit readers, miss cells, and (after the prune/upgrade)
/// the write handles. Its destructor is the SINGLE place the deferred LRU
/// bump runs, after every owned write buffer is finalized.
class CacheView
{
public:
    /// Virtual so a subclass with teardown work (`DiskCacheView`'s deferred LRU
    /// bump) runs through a `CacheViewPtr`; tiers without it use this class directly.
    virtual ~CacheView() = default;

    const VectorWithMemoryTracking<HitEntry> & hits() const { return hit_entries; }
    const VectorWithMemoryTracking<MissEntry> & misses() const { return miss_entries; }

    bool allHit() const { return miss_entries.empty(); }
    bool allMiss() const { return hit_entries.empty(); }

    /// PRUNE step: drop the miss at `index` (a cell the plan will not fill in
    /// this tier - e.g. fully covered by a faster tier). Runs between the probe
    /// and `openWriteBuffers`, so no write handle is ever opened for it.
    void dropMiss(size_t index) { miss_entries.erase(miss_entries.begin() + index); }

    /// Sorted, disjoint; hits + misses tile the lookup range (clamped to EOF /
    /// object end). EACH MISS RANGE IS ONE CELL. The builders (`planResidencyView`)
    /// write these directly.
    VectorWithMemoryTracking<HitEntry> hit_entries;
    VectorWithMemoryTracking<MissEntry> miss_entries;
};
using CacheViewPtr = std::unique_ptr<CacheView>;

/// Cache provider interface. `ReadPipeline` configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    virtual CacheTier tier() const = 0;

    /// Whether a miss on this tier is populated (write-through) or bypassed
    /// (read-only, writes are no-ops). Drives promotion: a range served from
    /// a slower tier is written up only into faster tiers that populate.
    virtual bool populatesOnMiss() const { return true; }

    /// Whether a cell is written WHOLE (first-writer-wins, never completed later -
    /// the page cache) vs incrementally appended (the filesystem cache). A
    /// whole-cell tier is a fill target only when a connection covers the ENTIRE
    /// cell; an incremental tier appends whatever prefix is covered.
    virtual bool fillsWholeCell() const { return false; }

    virtual String name() const = 0;

    /// Read-only residency probe over a (typically large) look-ahead range:
    /// hit read buffers (pinning their resident segments) + writer-null misses.
    /// EACH MISS RANGE IS ONE CELL of the tier - the provider owns the alignment
    /// policy (exact gaps for a bypass tier; boundary-aligned optimal cells for
    /// the filesystem cache; whole blocks for the page cache) - and the executor
    /// derives all fetch shaping from these cell edges. MUST NOT mutate the
    /// cache - a fully-resident range costs only the probe.
    virtual CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) = 0;

    /// UPGRADE step: open a write buffer into each of the view's surviving miss
    /// entries (the plan's `dropMiss` prune already ran), without re-probing
    /// residency. A no-op when `!populatesOnMiss()` - the writers stay null and
    /// every fill site skips null-writer entries.
    virtual void openWriteBuffers(
        const StoredObject & object, size_t object_file_offset, CacheView & view) = 0;
};

}
