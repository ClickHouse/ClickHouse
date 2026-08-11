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

/// Per-range buffer API: `resolve` splits a read into HIT ranges (each owns a `CacheReader`) and
/// MISS cells (each carries a `CacheWriter` when populating). Coordinates are FILE-LEVEL throughout.

/// Held, re-readable view of ONE resident (hit) file-level range. Owns the pin
/// that keeps its bytes alive; holds NO cursor (the executor's ChainedBuffers owns it).
class CacheReader
{
public:
    virtual ~CacheReader() = default;

    /// Cache-aligned range this buffer can serve (may be wider than the hit
    /// the plan asked for - the executor clamps).
    virtual ByteRange range() const = 0;

    /// Read `subrange` (within `range()`) as a ChainedBuffers of file-level nodes. Records
    /// `subrange` for the view's deferred LRU bump. A hit is the committed prefix; growth is the
    /// WRITER's story (`CacheWriter::committed`).
    virtual ChainedBuffers read(ByteRange subrange) = 0;
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

    /// Bytes within `range()` already committed (any order). By value: a snapshot under the writer's
    /// own lock, since the prefetch worker and foreground may write one writer concurrently.
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
    virtual ChainedBuffers read(ByteRange subrange) = 0;

    /// The result of `claim`: the downloader roles the calling thread holds over one window,
    /// plus the window's decomposition into runs to fetch (`to_fetch` - roles won, or
    /// uncoordinated bytes) and runs a sibling leads (`sibling_led` - already cached, or
    /// being downloaded by another reader; serve them from cache after). Move-only RAII:
    /// roles are thread-affine (the downloader id is the caller id), so the token must be
    /// created, written through, and destroyed on ONE thread; the destructor
    /// completes-and-releases exactly the roles this claim NEWLY won. Exceptions in the release
    /// are swallowed and logged.
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

    /// Acquire downloader roles for the cells overlapping `window` (clamped). The ONLY
    /// role-acquisition site - `write` never adopts a role, so live claims alone answer "which
    /// roles does this thread hold". Does NOT wait. Default: whole window to-fetch, nothing to release.
    virtual FillClaim claim(ByteRange window)
    {
        FillClaim c;
        c.to_fetch.push_back(window);
        return c;
    }

    /// Wait (bounded by `wait_for_concurrent_download_timeout_milliseconds`) until `subrange`'s
    /// bytes are committed by the sibling downloader, then serve them from this writer's own held
    /// segments (cache file). On a timeout the read can be short. Default: plain read (no wait).
    virtual ChainedBuffers waitAndReadSiblingLed(ByteRange subrange) { return read(subrange); }
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
        };

        Kind kind = Kind::Miss;
        /// Hit: one committed run. Miss: ONE cell of this tier containing the position (may
        /// overhang the asked span via grid rounding / object-end clamp).
        ByteRange range{};
        /// Hit only. A re-ask of the same run may return a fresh reader or a
        /// null one - the walker collects exactly one per run either way.
        CacheReaderPtr reader;
        /// Miss only: the cell's OPEN writer (populating tiers; null on
        /// bypass/read-only configurations).
        CacheWriterPtr writer;
    };

    /// Resolve `range` in ONE pass: every resolution covering it, in offset order, each once -
    /// hits carry readers. Whether a MISS carries an open writer is THE PROVIDER'S decision: a
    /// populating cache opens it here, a read-only / bypass cache returns it writer-less. The
    /// caller subtracts faster-tier hits BEFORE asking. Cells at the edges may overhang `range`
    /// (grid rounding, object-end clamp). Holds no per-call state, so a shared provider is safe
    /// to resolve from many threads (the `readBigAt` fan-out).
    virtual VectorWithMemoryTracking<Resolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) = 0;
};

}
