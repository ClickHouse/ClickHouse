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

    /// The bytes inside `range()` already committed, in any order. Returned by value: a snapshot taken
    /// under the writer's lock, because a background prefetch and the foreground read may write the
    /// same writer at the same time.
    virtual IntervalSet committed() const = 0;

    bool complete() const { return committed().subtract(range()).empty(); }

    /// Store the part of `data` that lies in `range()` and is not yet in `committed()`. Return the
    /// bytes that newly landed. Return 0 for bytes outside the range, already committed, in an
    /// unclaimed segment, on a reservation failure, or on bypass. Never throw for those; return a
    /// partial or zero count instead. On a tier that coordinates downloaders (the filesystem cache),
    /// a write lands only in segments that an open `claim` of the calling thread covers.
    virtual size_t write(ChainedBuffers data) = 0;

    /// Serve an already-committed sub-range from this writer's own held segments. Do not go to the source.
    virtual ChainedBuffers read(ByteRange subrange) = 0;

    /// The result of `claim`. It splits the window into `available` (already committed; read from the
    /// cache, never fetch) and `to_fetch` (the uncommitted tail whose downloader role this thread won;
    /// fetch and write it while the claim is open). Move-only and bound to one thread (the downloader
    /// id is the caller id). The destructor completes and releases exactly the roles this claim won,
    /// swallowing and logging any error.
    class FillClaim
    {
    public:
        FillClaim() = default;
        FillClaim(FillClaim && other) noexcept
            : available(std::move(other.available))
            , to_fetch(std::move(other.to_fetch))
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

        /// Already committed: read from the cache, never fetch.
        VectorWithMemoryTracking<ByteRange> available;
        /// The uncommitted tail whose downloader role this thread won: fetch and write it.
        VectorWithMemoryTracking<ByteRange> to_fetch;
        /// Completes and releases the newly-won roles. Never throws. Empty when the claim won nothing.
        std::function<void()> release;
    };

    /// Acquire downloader roles for the segments that overlap `window` (clamped). This is the only
    /// place that acquires a role; `write` never takes one. So the live claims alone tell which roles
    /// a thread holds. This does not wait. Default: fetch the whole window, release nothing.
    virtual FillClaim claim(ByteRange window)
    {
        FillClaim c;
        c.to_fetch.push_back(window);
        return c;
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

    /// Whether a miss on this tier populates the cache (write-through) or is bypassed (read-only;
    /// writes do nothing). This drives promotion: a range served from a slower tier is written up only
    /// into faster tiers that populate.
    virtual bool populatesOnMiss() const { return true; }

    /// Whether the tier writes each segment WHOLE (first-writer-wins, never completed later) or
    /// appends it incrementally. The filesystem cache appends. A whole-segment tier is a fill target
    /// only when one connection covers the ENTIRE segment; an incremental tier stores whatever prefix
    /// it covers.
    virtual bool fillsWholeSegment() const { return false; }

    virtual String name() const = 0;

    /// One step of the residency walk. The executor consumes these in order.
    struct Resolution
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
        /// Miss only: the segment's open writer. It is null on a bypass or read-only tier.
        CacheWriterPtr writer;
    };

    /// Resolve `range` in ONE pass. Return every resolution that covers it, in offset order, each
    /// once. Hits carry readers. The provider decides whether a miss carries an open writer: a
    /// populating tier opens it here, a read-only or bypass tier returns it writer-less. The caller
    /// subtracts faster-tier hits before it asks. Edge segments may overhang `range` (segment-boundary
    /// rounding, or the object-end clamp). Holds no per-call state, so many threads can resolve one
    /// shared provider at once (parallel `readBigAt`).
    virtual VectorWithMemoryTracking<Resolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) = 0;
};

}
