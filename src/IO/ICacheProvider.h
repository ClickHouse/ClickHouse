#pragma once

#include <IO/Rope.h>
#include <IO/IntervalSet.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <base/types.h>

#include <memory>
#include <vector>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

struct CacheLookupResult
{
    VectorWithMemoryTracking<ByteRange> hit_ranges;
    VectorWithMemoryTracking<ByteRange> miss_ranges;

    bool allHit() const { return miss_ranges.empty(); }
    bool allMiss() const { return hit_ranges.empty(); }
};

/// Handle returned by cache lookup. Pins results, provides get/put.
///
/// All `ByteRange` parameters and results are in FILE-LEVEL coordinates
/// (logical offsets inside the file the `ReadPipeline` is reading), so
/// the executor never has to translate. Caches whose internal key space
/// is per-object (`DiskCacheHandle` / FileCache) do their own
/// object_file_offset ↔ file-level translation internally.
///
/// RAII — destructor releases pins.
class ICacheHandle
{
public:
    virtual ~ICacheHandle() = default;

    /// Reports `hit_ranges` / `miss_ranges` in file-level coordinates at this
    /// cache's granularity.
    virtual CacheLookupResult status() const = 0;

    /// Read cached data as a rope slice. `range` must be within
    /// `hit_ranges`. The returned rope's nodes carry file-level
    /// `logical_offset`s.
    virtual Rope get(ByteRange range) = 0;

    /// Provide data for a miss range. `range` must be within
    /// `miss_ranges`; `data` must hold nodes in file-level coordinates.
    /// Returns the number of bytes that actually landed in the cache;
    /// can be less than `range.size` when we lost the downloader race
    /// on some segments, reservation failed (cache full), or the
    /// handle is in bypass mode (returns 0).
    ///
    /// LRU update ordering: implementations MUST defer the LRU-bump for
    /// every hit `get`-ed via this handle until destruction. The executor
    /// keeps the handle alive until after every `put` it intends to issue,
    /// so the bumps run AFTER the inserts — a hit that sits next to fresh
    /// inserts does not become "older" than them under LRU eviction. See
    /// `02944_dynamically_change_filesystem_cache_size` for the regression
    /// this ordering prevents.
    virtual size_t put(ByteRange range, Rope data) = 0;

    /// Opaque token that keeps one cache segment non-evictable until it is
    /// dropped. Null when there is nothing useful to pin. See ReaderExecutor's
    /// sequential-streaming pin (design: in-flight segment pin).
    using CacheSegmentPin = std::shared_ptr<void>;

    /// Pin the cache segment containing `file_offset` (file-level coordinates),
    /// so it survives eviction until the returned token is released. Only
    /// providers with append-only partial segments (DiskCache/FileCache)
    /// implement this; the default is a no-op. Returns null when the covering
    /// segment is absent, empty, fully downloaded, or this provider has no
    /// evictable-segment notion.
    virtual CacheSegmentPin pinSegmentAt(size_t /*file_offset*/) const { return nullptr; }
};

/// Which storage tier a cache provider represents. Drives per-tier byte
/// attribution in observability (`ReaderExecutorBytesFromPageCache` vs
/// `ReaderExecutorBytesFromFilesystemCache`).
enum class CacheTier
{
    PageCache,
    FilesystemCache,
};

/// ─────────────────────────────────────────────────────────────────────────────
/// NEW per-range buffer API (coexists with `ICacheHandle` during migration).
///
/// `lookup`/`planResidency` will return a `CacheView` that decomposes the request
/// into HIT ranges, each owning a held `CacheReadBuffer`, and MISS ranges, each
/// owning a held `CacheWriteBuffer`. The buffers are PER-RANGE and held by the
/// executor's plan across many read windows: a read buffer is freely re-readable;
/// a write buffer appends incrementally and owns its OWN segment ref(s) (not a
/// shared holder that a single `put` consumes), so it survives across windows and
/// is finalized only at destruction. Coordinates are FILE-LEVEL throughout.
/// ─────────────────────────────────────────────────────────────────────────────

/// Held, re-readable view of ONE resident (hit) file-level range. Owns the pin
/// that keeps its bytes alive for the buffer's whole lifetime. Re-readable any
/// number of times, any sub-range; holds NO cursor (the executor's Rope owns it).
class CacheReadBuffer
{
public:
    virtual ~CacheReadBuffer() = default;

    /// Cache-aligned file-level range this buffer can serve (segment/block-aligned;
    /// may be wider than the hit the plan asked for - the executor clamps).
    virtual ByteRange range() const = 0;

    /// Committed-prefix end, file-level. == `range().end()` for a fully-resident
    /// segment/block; for a partially-downloaded disk segment the LIVE current
    /// write offset mapped to file-level, re-evaluated each call so a concurrent
    /// downloader's growth is visible. The executor reads only up to `readable()`.
    virtual size_t readable() const = 0;

    /// Read `sub` (within `[range().offset, readable())`) as a Rope of file-level
    /// nodes. Records `sub` on the owning `CacheView` for the deferred LRU bump.
    virtual Rope read(ByteRange sub) = 0;
};

/// Held, incrementally-fillable target for ONE miss file-level range. Owns its own
/// writable segment ref(s) - NOT a shared holder a single `put` consumes - so it
/// appends across many windows and is finalized only at destruction.
class CacheWriteBuffer
{
public:
    virtual ~CacheWriteBuffer() = default;

    /// Cache-ALIGNED file-level range. May extend BEYOND the requested miss range
    /// (head/prefix below offset for disk; whole block for page).
    virtual ByteRange range() const = 0;

    /// Bytes within `range()` already committed by this buffer (any order). The
    /// next byte to fetch-and-store is the first offset in `range()` not committed.
    virtual const IntervalSet & committed() const = 0;

    /// True once `range()` is fully committed; further `write` over committed bytes
    /// is a no-op returning 0.
    virtual bool complete() const = 0;

    /// Store the portion of `data` within (`range()` minus `committed()`), any
    /// order. Returns bytes that newly landed (== bytes added to `committed()`); 0
    /// for bytes outside `range()`, already committed, a lost downloader race,
    /// reservation failure, bypass mode, or first-writer-wins. NEVER throws on
    /// those - degrades to a partial or zero return.
    virtual size_t write(Rope data) = 0;

    /// Serve an already-committed sub-range from this buffer's own held segment /
    /// cells, without a source round-trip. `sub` must lie in `committed()`.
    virtual Rope read(ByteRange sub) = 0;

    /// Opaque token keeping the partial segment under `frontier` non-evictable
    /// while the live source connection streams into it (the bare segment ref this
    /// buffer already holds). Null unless that segment is partially-downloaded with
    /// a committed prefix and not detached. Default no-op (e.g. page cache).
    using CacheSegmentPin = std::shared_ptr<void>;
    virtual CacheSegmentPin pin(size_t /*frontier*/) const { return nullptr; }
};

using CacheReadBufferPtr = std::unique_ptr<CacheReadBuffer>;
using CacheWriteBufferPtr = std::unique_ptr<CacheWriteBuffer>;

/// One resident range + its held read buffer.
struct HitEntry { ByteRange range; CacheReadBufferPtr reader; };
/// One miss range (cache-aligned) + its held write buffer (null on read-only/bypass).
struct MissEntry { ByteRange range; CacheWriteBufferPtr writer; };

/// Decomposed lookup result: the hit/miss map with per-range buffers, held by the
/// plan across windows. Its destructor is the SINGLE place the deferred LRU bump
/// runs, and it must run AFTER finalizing every owned write buffer.
class CacheView
{
public:
    virtual ~CacheView() = default;

    /// Sorted, disjoint; hits + misses tile the lookup range (clamped to EOF /
    /// object end). Miss ranges are cache-ALIGNED and may extend beyond the request.
    virtual const std::vector<HitEntry> & hits() const = 0;
    virtual const std::vector<MissEntry> & misses() const = 0;

    bool allHit() const { return misses().empty(); }
    bool allMiss() const { return hits().empty(); }
};
using CacheViewPtr = std::unique_ptr<CacheView>;

/// Cache provider interface. `ReadPipeline` configures the chain.
class ICacheProvider
{
public:
    virtual ~ICacheProvider() = default;

    /// The storage tier this provider represents, used to attribute served
    /// bytes to the right counter.
    virtual CacheTier tier() const = 0;

    /// Lookup a range of `object` inside the file.
    ///   - `object` is the per-object identity (`remote_path` / `local_path`).
    ///     Per-object caches (`DiskCacheProvider`) derive their cache key
    ///     and origin from it; file-level caches (`PageCacheProvider`)
    ///     ignore it.
    ///   - `object_file_offset` is the offset inside the logical file
    ///     where `object` starts. Used by per-object caches to translate
    ///     the file-level `range_in_file` to / from their object-local
    ///     internal key space.
    ///   - `range_in_file` is the request range in file-level
    ///     coordinates — `[object_file_offset,
    ///     object_file_offset + object.bytes_size)` is the slice that
    ///     belongs to this `object`.
    virtual std::unique_ptr<ICacheHandle> lookup(
        const StoredObject & object,
        size_t object_file_offset,
        ByteRange range_in_file) = 0;

    /// Read-only residency query over a (typically large) look-ahead range,
    /// intended to be held across many stream windows. Same arguments and
    /// coordinate space as `lookup`; the returned handle reports resident bytes
    /// as `status().hit_ranges` and gaps as `status().miss_ranges`, and pins the
    /// resident segments for its lifetime so they can be streamed via `get`.
    ///
    /// Unlike `lookup`, `planResidency` MUST NOT mutate the cache: it never
    /// creates segments and its `put` is a no-op. This is the basis of
    /// plan-then-stream — discover resident ranges once over the look-ahead
    /// window, stream them, and fill gaps through a separate `lookup` + `put`.
    /// On a fully-resident range it therefore costs nothing beyond the residency
    /// probe, which is the whole point: it avoids the per-window `getOrSet`
    /// metadata churn that `lookup` incurs.
    ///
    /// The default delegates to `lookup`, which is correct for providers whose
    /// `lookup` is already a non-mutating, pinning probe (e.g. `PageCacheProvider`,
    /// and the in-memory test providers). Providers whose `lookup` mutates
    /// (`DiskCacheProvider`, via `getOrSet`) override this with a read-only path.
    virtual std::unique_ptr<ICacheHandle> planResidency(
        const StoredObject & object,
        size_t object_file_offset,
        ByteRange range_in_file)
    {
        return lookup(object, object_file_offset, range_in_file);
    }

    /// Whether a miss on this tier is populated (write-through) or bypassed
    /// (read-only, `put` is a no-op). Drives `WritePlan` promotion: a range served
    /// from a slower tier is written up into the faster tiers that miss it, but
    /// only the ones that populate. Mirrors the per-tier
    /// `read_from_{filesystem,page}_cache_if_exists_otherwise_bypass_cache` setting.
    /// Default: write-through.
    virtual bool populatesOnMiss() const { return true; }

    /// Granularity the foreground rounds the HEAD of a fetch DOWN to, so a read that
    /// starts mid-cell fills that cell's aligned prefix (the segment/block this read
    /// lands inside is created at the aligned floor, and its write buffer appends from
    /// `cwo` = the aligned floor - a fetch that skipped the prefix would land nothing).
    /// The aligned-prefix slack outside the requested window is counted as over-read.
    /// `1` disables head over-read. Disk: `boundary_alignment`. Page: `block_size`.
    virtual size_t fetchHeadAlignment() const { return 1; }

    /// Granularity the foreground rounds the TAIL of a fetch UP to. Only tiers that
    /// require WHOLE-cell writes need this (a page-cache block is first-writer-wins, so
    /// a partially-written block can never be completed later). `1` disables tail
    /// over-read - the right choice for incrementally-fillable tiers (a disk segment
    /// appends at `cwo` and is continued by the next window). Page: `block_size`.
    virtual size_t fetchTailAlignment() const { return 1; }

    virtual String name() const = 0;

    /// ── NEW per-range buffer API (see CacheView above; coexists with `lookup`) ──
    /// Build the hit/miss map AND open write buffers for every miss range (only
    /// when `populatesOnMiss()`; a bypass tier returns `MissEntry{range, nullptr}`).
    /// Misses are returned cache-ALIGNED (alignment folded in - no separate
    /// `alignToCaches`). Mutating. Default throws until the provider implements it.
    virtual CacheViewPtr lookupView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file);

    /// Read-only residency probe: hit read buffers + writer-null cache-aligned
    /// misses. MUST NOT mutate. Default delegates to `lookupView`; the disk
    /// provider overrides with a read-only path.
    virtual CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file);

    /// Open ONLY the write buffers for already-known cache-aligned miss ranges,
    /// without re-probing residency (replaces the executor's `ensureWriteHandle`).
    /// Returns empty when `!populatesOnMiss()`. Default throws until implemented.
    virtual std::vector<MissEntry> openWriteBuffers(
        const StoredObject & object, size_t object_file_offset,
        const std::vector<ByteRange> & aligned_miss_ranges);
};

}
