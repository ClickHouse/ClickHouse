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

/// Which storage tier a cache provider represents. The metrics use this to attribute bytes per tier.
enum class CacheTier
{
    PageCache,
    FilesystemCache,
};

/// Per-range buffer API for the cache tiers. `resolve` splits a read into hit ranges and miss
/// segments. Each hit owns a `CacheReader`. Each miss carries a `CacheWriter` when the tier
/// populates. All coordinates are file-level.

/// A held, re-readable view of ONE hit range (bytes already in the cache). It owns the pin that
/// keeps its bytes alive. It holds no cursor; the executor's `ChainedBuffers` owns the cursor.
class CacheReader
{
public:
    virtual ~CacheReader() = default;

    /// The cache-aligned range this buffer can serve. It may be wider than the requested hit. The
    /// executor clamps it.
    virtual ByteRange range() const = 0;

    /// Read `subrange` (inside `range()`) as a `ChainedBuffers` of file-level nodes. Record `subrange`
    /// so the view can bump its cache priority later. A reader serves only the committed prefix; the
    /// writer tracks any later growth (`CacheWriter::committed`).
    virtual ChainedBuffers read(ByteRange subrange) = 0;
};

/// A held, fill-as-you-go target for ONE miss range. It owns its own writable segment references,
/// so it can append across many windows. It finalizes only at destruction.
class CacheWriter
{
public:
    virtual ~CacheWriter() = default;

    /// The cache-aligned range. It may extend beyond the requested miss range.
    virtual ByteRange range() const = 0;

    /// The end of the committed prefix `[range().offset, committed())` (a segment fills append-only, so
    /// what is committed is always one contiguous prefix). Always within `[range().offset, range().end()]`:
    /// `range().offset` when nothing is committed, `range().end()` when complete. A snapshot: a background
    /// prefetch and the foreground read may grow it concurrently.
    virtual size_t committed() const = 0;

    bool complete() const { return committed() == range().end(); }

    /// Whether the tier stores each segment WHOLE (one all-or-nothing write) or appends it
    /// incrementally. Mirrors `ICacheProvider::fillsWholeSegment`.
    virtual bool fillsWholeSegment() const { return false; }

    /// Whether this reader fills one write range: `bool(role)` is true iff it should fetch the
    /// uncommitted part and `write` it. Move-only RAII. A coordinating tier also carries the downloader
    /// role it won, and the destructor completes and releases it (never throws). A non-coordinating tier
    /// holds it trivially and releases nothing, so two readers can both fill and the cache keeps the
    /// first write. Only a `CacheWriter` mints one (see `makeFillRole`), so a `FillRole` argument to
    /// `write` proves the caller may write. Bound to one thread (the downloader id is the caller id):
    /// create, write, and destroy it on the same thread.
    class FillRole
    {
    public:
        FillRole() = default;
        FillRole(FillRole && other) noexcept
            : held(std::exchange(other.held, false))
            , release(std::exchange(other.release, nullptr))
        {
        }
        FillRole & operator=(FillRole && other) noexcept
        {
            if (this != &other)
            {
                reset();
                held = std::exchange(other.held, false);
                release = std::exchange(other.release, nullptr);
            }
            return *this;
        }
        FillRole(const FillRole &) = delete;
        FillRole & operator=(const FillRole &) = delete;
        ~FillRole() { reset(); }

        explicit operator bool() const noexcept { return held; }

        void reset() noexcept
        {
            held = false;
            if (auto r = std::exchange(release, nullptr))
                r();
        }

    private:
        friend class CacheWriter;
        FillRole(bool held_, std::function<void()> release_) : held(held_), release(std::move(release_)) {}
        bool held = false;
        std::function<void()> release;
    };

protected:
    /// Mint a `FillRole`. Only a `CacheWriter` subclass may authorize a write.
    static FillRole makeFillRole(bool held, std::function<void()> release) { return FillRole(held, std::move(release)); }

public:
    /// Store the part of `data` inside `range()` and not yet in `committed()`, under the held `role`
    /// (the caller's proof that it may write; `write` never takes a role). Return the bytes that newly
    /// landed. Return 0 for bytes outside the range, already committed, on a reservation failure, or on
    /// bypass; never throw for those.
    virtual size_t write(ChainedBuffers data, const FillRole & role) = 0;

    /// Serve an already-committed sub-range from this writer's own held segments. Do not go to the source.
    virtual ChainedBuffers read(ByteRange subrange) = 0;

    /// Decide whether this reader fills the writer's range, and on a coordinating tier acquire the
    /// downloader role for it. The only place a role is taken; `write` never takes one. Return a held
    /// `FillRole` iff there is an uncommitted part this reader must fill; an empty one means the range is
    /// already committed (read it via `committed()`) or a concurrent downloader leads it (`waitAndRead`
    /// it). Do not wait. Default: a non-coordinating tier, always held.
    virtual FillRole takeFillRole()
    {
        return makeFillRole(/*held=*/true, /*release=*/nullptr);
    }

    /// Wait (bounded by `wait_for_concurrent_download_timeout_milliseconds`) until the concurrent
    /// downloader commits `subrange`, then serve those bytes from this writer's own held segments. On
    /// a timeout the read can be short. Default: plain read (no wait).
    virtual ChainedBuffers waitAndRead(ByteRange subrange) { return read(subrange); }
};

using CacheReaderPtr = std::unique_ptr<CacheReader>;
using CacheWriterPtr = std::unique_ptr<CacheWriter>;

/// Cache provider interface. `ReadPipeline` configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    virtual CacheTier tier() const = 0;

    /// Whether the tier writes each segment WHOLE (first-writer-wins, never completed later) or
    /// appends it incrementally. The filesystem cache appends. A whole-segment tier is a fill target
    /// only when one connection covers the ENTIRE segment; an incremental tier stores whatever prefix
    /// it covers.
    virtual bool fillsWholeSegment() const { return false; }

    virtual String name() const = 0;

    /// One step of the residency walk. The executor consumes these in order.
    struct CacheResolution
    {
        enum class Kind : uint8_t
        {
            Hit,
            Miss,
        };

        Kind kind = Kind::Miss;
        /// Hit: one committed run. Miss: ONE segment of this tier that contains the position. A miss
        /// segment may overhang the asked span (segment-boundary rounding, or the object-end clamp).
        ByteRange range{};
        /// Hit only. A re-ask of the same run may return a fresh reader or a null one; the executor
        /// keeps exactly one per run either way.
        CacheReaderPtr reader;
        /// Miss only: the segment's open writer. Null when the segment will not be populated.
        CacheWriterPtr writer;
    };

    /// Resolve `range` in ONE pass. Return every resolution that covers it, in offset order, each
    /// once. Hits carry readers. A miss carries an open writer when this tier populates and the
    /// segment can accept bytes, and is writer-less otherwise. The caller subtracts faster-tier hits
    /// before it asks. Edge segments may overhang `range` (segment-boundary rounding, or the
    /// object-end clamp). Holds no per-call state, so many threads can resolve one shared provider at
    /// once (parallel `readBigAt`). `range` is file-space; `object_offset` is `range.offset`'s
    /// object-local position (so the object's file base is `range.offset - object_offset`).
    virtual VectorWithMemoryTracking<CacheResolution> resolve(
        const StoredObject & object, size_t object_offset, ByteRange range) = 0;
};

}
