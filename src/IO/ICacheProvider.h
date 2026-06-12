#pragma once

#include <IO/Rope.h>
#include <IO/IntervalSet.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

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
/// that keeps its bytes alive; holds NO cursor (the executor's Rope owns it).
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

    /// Read `sub` (within `[range().offset, readable())`) as a Rope of
    /// file-level nodes. Records `sub` for the view's deferred LRU bump.
    virtual Rope read(ByteRange sub) = 0;
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
    virtual const IntervalSet & committed() const = 0;

    virtual bool complete() const = 0;

    /// Store the portion of `data` within `range()` minus `committed()`.
    /// Returns the bytes that newly landed; 0 for bytes outside the range,
    /// already committed, a lost downloader race, reservation failure or
    /// bypass - NEVER throws on those, degrades to a partial or zero return.
    virtual size_t write(Rope data) = 0;

    /// Serve an already-committed sub-range from this buffer's own held
    /// segments/cells, without a source round-trip.
    virtual Rope read(ByteRange sub) = 0;

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
/// One miss range (cache-aligned) + its held write buffer (null on read-only/bypass).
struct MissEntry { ByteRange range; CacheWriterPtr writer; };

/// Decomposed lookup result, held by the plan across windows. Its destructor
/// is the SINGLE place the deferred LRU bump runs, after every owned write
/// buffer is finalized.
class CacheView
{
public:
    virtual ~CacheView() = default;

    /// Sorted, disjoint; hits + misses tile the lookup range (clamped to
    /// EOF / object end). Miss ranges are cache-ALIGNED.
    virtual const VectorWithMemoryTracking<HitEntry> & hits() const = 0;
    virtual const VectorWithMemoryTracking<MissEntry> & misses() const = 0;

    bool allHit() const { return misses().empty(); }
    bool allMiss() const { return hits().empty(); }
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

    /// Granularity the fetch HEAD is rounded DOWN to, so a read starting
    /// mid-cell fills the cell's aligned prefix. The slack is counted as
    /// over-read. `1` disables. Disk: `boundary_alignment`; page: `block_size`.
    virtual size_t fetchHeadAlignment() const { return 1; }

    /// Granularity the fetch TAIL is rounded UP to. Needed only by tiers with
    /// whole-cell writes (a page block is first-writer-wins, never completed
    /// later); `1` for incrementally-fillable tiers.
    virtual size_t fetchTailAlignment() const { return 1; }

    virtual String name() const = 0;

    /// Read-only residency probe over a (typically large) look-ahead range:
    /// hit read buffers (pinning their resident segments) + writer-null
    /// cache-aligned misses. MUST NOT mutate the cache - a fully-resident
    /// range costs only the probe. Default throws until implemented.
    virtual CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file);

    /// Open ONLY the write buffers for already-known cache-aligned miss
    /// ranges, without re-probing residency. Empty when `!populatesOnMiss()`.
    /// Default throws until implemented.
    virtual VectorWithMemoryTracking<MissEntry> openWriteBuffers(
        const StoredObject & object, size_t object_file_offset,
        const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges);
};

}
