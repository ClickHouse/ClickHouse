#pragma once

#include <IO/ChainedBuffers.h>
#include <algorithm>
#include <IO/IntervalSet.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <functional>
#include <memory>
#include <optional>
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

/// Per-range buffer API: `resolve` decomposes a read into HIT ranges (each
/// owning a held `CacheReader`) and MISS cells, each carrying a held
/// `CacheWriter` when the provider populates. The buffers are held by the
/// executor's plan across many read windows. Coordinates are FILE-LEVEL
/// throughout.

/// Held, re-readable view of ONE resident (hit) file-level range. Owns the pin
/// that keeps its bytes alive; holds NO cursor (the executor's ChainedBuffers owns it).
class CacheReader
{
public:
    virtual ~CacheReader() = default;

    /// Cache-aligned range this buffer can serve (may be wider than the hit
    /// the plan asked for - the executor clamps).
    virtual ByteRange range() const = 0;

    /// Read `sub` (within `range()`) as a ChainedBuffers of
    /// file-level nodes. Records `sub` for the view's deferred LRU bump.
    /// A hit is readable in full: the probe splits a partially-downloaded
    /// segment at its write offset, so the hit is the committed prefix and
    /// growth is the WRITER's story (`CacheWriter::committed`).
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
            : available(std::move(other.available))
            , to_fetch(std::move(other.to_fetch))
            , sibling_led(std::move(other.sibling_led))
            , release(std::exchange(other.release, nullptr))
        {
        }
        FillClaim & operator=(FillClaim && other) noexcept
        {
            if (this != &other)
            {
                reset();
                available = std::move(other.available);
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

        /// Already committed in this thread's own cells (a prior downloader's partial
        /// that we resumed): read it from cache, never fetch it from the source. Carries
        /// NO contention meaning - unlike `sibling_led`, it does not flag `m.contended`.
        VectorWithMemoryTracking<ByteRange> available;
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

    /// One step of the residency walk (the executor's iterator consumes this).
    struct Resolution
    {
        enum class Kind : uint8_t
        {
            Hit,
            Miss,
            End,
        };

        Kind kind = Kind::End;
        /// Hit: one committed run (a probe hit entry - split at the writer
        /// frontier). Miss: ONE cell of this tier containing the position
        /// (object-end-clamped, so it may overhang the asked span). End: the
        /// position is past the object's tiling.
        ByteRange range{};
        /// Hit only. A re-ask of the same run may return a fresh reader or a
        /// null one - the walker collects exactly one per run either way.
        CacheReaderPtr reader;
        /// Miss only: the cell's OPEN writer (populating tiers; null on
        /// bypass/read-only configurations).
        CacheWriterPtr writer;
    };

    /// Resolve `range` in ONE pass: every resolution covering it, in offset
    /// order, each once - hits carry readers. Whether a MISS carries an open
    /// writer is THE PROVIDER'S decision, not the caller's: a populating cache
    /// opens the writer at this call (one cache transaction resolves and
    /// allocates); a read-only / bypass cache returns the miss writer-less. The
    /// caller subtracts faster-tier hits BEFORE asking, so no writer opens for a
    /// pruned cell. Cells at the range edges may overhang it (grid rounding,
    /// object-end clamping). MUST NOT mutate the cache beyond that populate
    /// decision (a read-only cache never does). Holds no per-call state, so a
    /// shared provider is safe to resolve from many threads (the `readBigAt`
    /// fan-out).
    virtual VectorWithMemoryTracking<Resolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) = 0;
};

}
