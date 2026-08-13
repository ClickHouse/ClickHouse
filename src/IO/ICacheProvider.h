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

    /// A held downloader LEAD role over one write range. Move-only RAII: the destructor
    /// completes-and-releases the role (noexcept - the provider wraps the release body in
    /// try/catch and logs). `bool(claim)` is true iff this thread is AUTHORIZED to write the
    /// range: on a coordinating tier (disk cache) it won the role AND there is an uncommitted
    /// tail to fill; on a non-coordinating tier (page cache) it is trivially true. A provider is
    /// the only source of a held `Claim` (see `makeClaim`), so a `Claim` param on `write` proves
    /// the caller acquired the role first. Roles are thread-affine (the downloader id is the
    /// caller id), so the token must be created, written through, and destroyed on ONE thread.
    class Claim
    {
    public:
        Claim() = default;
        Claim(Claim && other) noexcept
            : held(std::exchange(other.held, false))
            , release(std::exchange(other.release, nullptr))
        {
        }
        Claim & operator=(Claim && other) noexcept
        {
            if (this != &other)
            {
                reset();
                held = std::exchange(other.held, false);
                release = std::exchange(other.release, nullptr);
            }
            return *this;
        }
        Claim(const Claim &) = delete;
        Claim & operator=(const Claim &) = delete;
        ~Claim() { reset(); }

        explicit operator bool() const noexcept { return held; }

        void reset() noexcept
        {
            held = false;
            if (auto r = std::exchange(release, nullptr))
                r();
        }

    private:
        friend class CacheWriter;
        Claim(bool held_arg, std::function<void()> release_arg) : held(held_arg), release(std::move(release_arg)) {}
        bool held = false;
        std::function<void()> release;
    };

    /// The result of `claimLeadRole`: the committed prefix and the lead role. The caller derives
    /// the uncommitted tail `[available.end(), range.end())` - OURS to fetch if `bool(claim)`,
    /// else a sibling's to wait on. `available` is a single contiguous prefix because a segment is
    /// written append-only at its write offset.
    struct Lead
    {
        ByteRange available;   /// committed prefix within `range` (size 0 == nothing committed yet)
        Claim claim;           /// held iff there is an uncommitted tail this thread must fill
    };

protected:
    /// Mint a `Claim`. Only `CacheWriter` and its provider subclasses may authorize a write.
    static Claim makeClaim(bool held, std::function<void()> release) { return Claim(held, std::move(release)); }

public:
    /// Store the portion of `data` within `range()` minus `committed()`, under the held `claim`
    /// (the caller's proof it holds this range's lead role - `write` never acquires one). Returns
    /// the bytes that newly landed; 0 for bytes outside the range, already committed, reservation
    /// failure or bypass - NEVER throws on those, degrades to a partial or zero return.
    virtual size_t write(ChainedBuffers data, const Claim & claim) = 0;

    /// Serve an already-committed sub-range from this buffer's own held
    /// segments/cells, without a source round-trip.
    virtual ChainedBuffers read(ByteRange sub) = 0;

    /// Acquire the lead role for the cache segment overlapping `range` (clamped internally) and
    /// report the committed prefix. The ONLY role-acquisition site: `write` never adopts a role,
    /// even one a sibling freed mid-window - a freed cell is picked up by the NEXT claim, which
    /// keeps "which roles does this thread hold" answerable from the live claims alone. Holds the
    /// role ONLY when there is an uncommitted tail to fill; if the committed prefix already covers
    /// `range` it releases immediately and returns an empty `Claim`. Does NOT wait. Default: no
    /// coordination - nothing committed, the role trivially held (e.g. page cache).
    virtual Lead claimLeadRole(ByteRange range)
    {
        return Lead{ByteRange{range.offset, 0}, makeClaim(/*held=*/true, /*release=*/nullptr)};
    }

    /// Wait until `sub`'s bytes are committed by the sibling downloader, then serve them
    /// from this writer's own held segments (cache file). Default: plain read (no wait).
    virtual ChainedBuffers waitAndReadSiblingLed(ByteRange sub) { return read(sub); }

    /// True if `frontier` lands inside a segment this buffer is still filling (a partial).
    /// Diagnostic only: the plan's writer holder is what keeps such a segment non-releasable
    /// across eviction / a cache drop - this merely gates the read-ahead pause failpoint.
    /// Default false (e.g. page cache).
    virtual bool frontierInPartial(size_t /*frontier*/) const { return false; }
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
