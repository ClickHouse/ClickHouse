#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/CacheBase.h>
#include <IO/ReadBufferFromFileBase.h>

#include <mutex>

namespace DB
{

/// Bounded keep-alive cache of open cache-segment readers, at most one per
/// segment path, LRU/SLRU-evicted (`EqualWeightFunction` makes the size cap a
/// count). The readers are held only as ANCHORS, never read through:
/// `createReadBufferFromFileBase` (pread) shares descriptors via
/// `OpenedFileCache`, whose `weak_ptr` drops an fd once the last reader dies, so
/// sequential windows would otherwise reopen the same segment every time.
/// Keeping one recently-used reader per segment alive keeps its `OpenedFile`
/// alive, so the next `create` is an `OpenedFileCache` hit (no `open` syscall).
/// `CacheBase` is internally synchronized, and since the anchors are never read,
/// each read uses its own fresh reader — nothing to race on.
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// One held cache-segment reader, reused across windows by the sequential read
/// path so a warm stream stops rebuilding a `ReadBufferFromFileBase` per `get`.
/// Concurrency: a caller either takes the held reader (exclusive until check-in)
/// or, when it is busy / for another segment, opens its own fresh reader — so the
/// reader object is never used by two threads at once. That is what makes this
/// safe under the concurrent `readBigAt` fan-out that shares one
/// `DiskCacheProvider`; under that fan-out the slot just falls back to fresh
/// readers (correct, reuse degraded), while a lone sequential stream reuses fully.
struct StreamingReaderSlot
{
    std::mutex mutex;
    String path;
    std::shared_ptr<ReadBufferFromFileBase> reader;
    /// File offset the held reader is positioned at (where its next read lands).
    size_t next_position = 0;
    bool checked_out = false;

    /// Take the held reader ONLY when it is free, for `p`, AND already sitting at
    /// `offset` — i.e. the next read is exactly contiguous, so the caller needs no
    /// `seek`. We never `seek` a reused reader: it was last driven in
    /// external-buffer (`set`) mode, whose stale working-buffer coordinates make
    /// `seek`'s in-buffer shortcut mis-position (returns EOF). Non-contiguous reads
    /// open a fresh reader instead (clean `seek`).
    std::shared_ptr<ReadBufferFromFileBase> tryCheckout(const String & p, size_t offset)
    {
        std::lock_guard lock(mutex);
        if (checked_out || !reader || path != p || next_position != offset)
            return nullptr;
        checked_out = true;
        return reader;
    }

    /// Return `r` as the held reader for `p`, positioned at `next_pos`, free.
    void checkin(const String & p, std::shared_ptr<ReadBufferFromFileBase> r, size_t next_pos)
    {
        std::lock_guard lock(mutex);
        path = p;
        reader = std::move(r);
        next_position = next_pos;
        checked_out = false;
    }

    /// Drop the held reader (e.g. a read threw): never reuse a faulted reader.
    void abandon()
    {
        std::lock_guard lock(mutex);
        reader = nullptr;
        checked_out = false;
    }
};

/// Held, re-readable view of ONE resident (hit) file-level range, backed by a
/// shared read-only `FileSegmentsHolder` built once by `planResidencyView`. All
/// hit read buffers of a single view share that holder (so the segments stay
/// pinned for the view's lifetime). Re-readable any sub-range, any number of
/// times; holds NO cursor and never mutates the cache. See `CacheReader`.
class DiskCacheReader : public CacheReader
{
public:
    DiskCacheReader(
        std::shared_ptr<FileSegmentsHolder> holder_,
        ByteRange range_in_file,
        size_t object_file_offset_,
        ThrottlerPtr local_throttler_,
        ReaderAnchorCache * anchors_,
        StreamingReaderSlot * stream_slot_,
        std::vector<ByteRange> * hits_to_touch_sink_);

    ByteRange range() const override { return hit_range; }
    size_t readable() const override;
    Rope read(ByteRange sub) override;

private:
    std::shared_ptr<FileSegmentsHolder> holder;
    ByteRange hit_range;
    size_t object_file_offset;
    ThrottlerPtr local_throttler;
    ReaderAnchorCache * anchors = nullptr;
    StreamingReaderSlot * stream_slot = nullptr;
    /// Back-pointer to the owning `DiskCacheView`'s deferred-bump list. Each
    /// `read` appends its `sub` here so the view's destructor can bump the LRU
    /// after all writes (see `~DiskCacheView`). Not owned; the view outlives this.
    std::vector<ByteRange> * hits_to_touch_sink = nullptr;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// Held, incrementally-fillable target for ONE miss file-level range. Owns its
/// OWN `FileSegmentsHolder` (from a single `getOrSet` over its cache-aligned
/// range, built by `openWriteBuffers`), so it appends across many windows and is
/// finalized only at destruction — when the held holder's destructor completes
/// each segment (this buffer being the last owner shrinks a partial segment to
/// its downloaded size and releases the reserved tail). See `CacheWriter`.
class DiskCacheWriter : public CacheWriter
{
public:
    DiskCacheWriter(
        FileCachePtr cache_,
        size_t object_file_offset_,
        const FilesystemCacheSettings & cache_settings_,
        FileSegmentsHolderPtr holder_,
        ByteRange aligned_range_in_file);

    ByteRange range() const override { return aligned_range; }
    const IntervalSet & committed() const override { return committed_ranges; }
    bool complete() const override;
    size_t write(Rope data) override;
    Rope read(ByteRange sub) override;
    CacheWriter::CacheSegmentPin pin(size_t frontier) const override;

private:
    bool tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset);

    FileCachePtr cache;
    size_t object_file_offset;
    FilesystemCacheSettings cache_settings;
    FileSegmentsHolderPtr holder;
    IntervalSet committed_ranges;
    ByteRange aligned_range;
    LoggerPtr log = getLogger("DiskCacheWriter");
};

/// Read-only `CacheView` returned by `DiskCacheProvider::planResidencyView`.
/// Holds the shared read-only holder (keeps hit segments pinned and shared by
/// every `DiskCacheReader` it owns) plus the deferred-LRU-bump context. Its
/// destructor runs the bump over all ranges the read buffers recorded. Misses
/// carry `writer == nullptr` (this view never opens writers — `openWriteBuffers`
/// does that separately).
class DiskCacheView : public CacheView
{
public:
    DiskCacheView(
        std::shared_ptr<FileSegmentsHolder> read_holder_,
        FileCachePtr cache_,
        FileCacheKey cache_key_,
        FileCacheOriginInfo origin_,
        size_t object_file_offset_);

    ~DiskCacheView() override;

    const std::vector<HitEntry> & hits() const override { return hit_entries; }
    const std::vector<MissEntry> & misses() const override { return miss_entries; }

    std::vector<HitEntry> hit_entries;
    std::vector<MissEntry> miss_entries;
    /// Appended to by the owned read buffers' `read` calls; consumed by the dtor.
    std::vector<ByteRange> hits_to_touch;

private:
    std::shared_ptr<FileSegmentsHolder> read_holder;
    FileCachePtr cache;
    FileCacheKey cache_key;
    FileCacheOriginInfo origin;
    size_t object_file_offset;
    LoggerPtr log = getLogger("DiskCacheView");
};


/// ICacheProvider wrapping FileCache.
///
/// Safe for concurrent use: `planResidencyView`/`openWriteBuffers` only read
/// immutable members and the internally-locked `FileCache`, each returned view /
/// buffer is per-call, and the only shared mutable state — the `ReaderAnchorCache`
/// keep-alive set — is
/// internally synchronized. This matters because `PipelineReadBuffer::readBigAt`
/// fans out concurrent reads over one shared provider.
///
/// Per-object cache identity:
///   - When `custom_cache_key` is set, that key is used for every lookup
///     (single-object, etag-keyed flow such as `StorageObjectStorageSource`).
///   - Otherwise the per-object `FileCacheKey::fromPath(object.remote_path)`
///     is used — supports multi-object gather mode where each object has
///     its own cache identity.
///
/// Per-object origin classification:
///   - When `custom_origin` is set, that origin is used.
///   - Otherwise `cache->getCommonOriginWithSegmentKeyType(object.local_path)`
///     is called per lookup — preserves the `Data` / `System` segment
///     classification (by file extension) that legacy `CachedObjectStorage`
///     applied per object.
class DiskCacheProvider : public ICacheProvider
{
public:
    /// `query_id` is required to enforce `filesystem_cache_max_download_size`:
    /// the provider keeps a `FileCache::QueryContextHolder` alive for its
    /// whole lifetime so `FileCache::tryReserve` (called inside
    /// `CacheWriter::write`) finds the matching per-query budget when it
    /// looks up `CurrentThread::getQueryId()`. Passing an empty `query_id`
    /// (or running with `filesystem_cache_max_download_size = 0`) is
    /// equivalent to no holder — the cache then has no per-query limit.
    DiskCacheProvider(
        FileCachePtr cache_,
        const FilesystemCacheSettings & cache_settings_,
        const String & query_id_ = {},
        ThrottlerPtr local_throttler_ = nullptr,
        std::optional<FileCacheKey> custom_cache_key_ = std::nullopt,
        std::optional<FileCacheOriginInfo> custom_origin_ = std::nullopt);

    String name() const override { return "DiskCache"; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }
    bool populatesOnMiss() const override { return !cache_settings.read_if_exists_otherwise_bypass; }

    /// A miss segment is created at the `boundary_alignment` floor of the read and its
    /// write buffer appends from that floor, so the fetch head must reach it. The tail
    /// fills incrementally (the next window continues the segment), so no tail rounding.
    size_t fetchHeadAlignment() const override { return cache->getBoundaryAlignment(); }

    /// Read-only residency probe for the new per-range buffer API. Builds a
    /// `DiskCacheView` over a single `cache->get` (no segment creation): each
    /// resident sub-range becomes a `HitEntry` with a shared-holder-backed
    /// `DiskCacheReader`, each gap a cache-aligned `MissEntry` with
    /// `writer == nullptr`. Hits + misses tile the request. A concurrently-
    /// `DOWNLOADING` segment credits its committed prefix `[seg.left, cwo)` as a
    /// hit and misses only the tail. See `ICacheProvider::planResidencyView`.
    CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override;

    /// Open write buffers for the already-known cache-aligned miss ranges (one
    /// `getOrSet` per range, the held holder owned by each `DiskCacheWriter`).
    /// Returns empty when `!populatesOnMiss()`. See `ICacheProvider::openWriteBuffers`.
    std::vector<MissEntry> openWriteBuffers(
        const StoredObject & object, size_t object_file_offset,
        const std::vector<ByteRange> & aligned_miss_ranges) override;

private:
    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    /// Forwarded into each `DiskCacheReader` so cache-file reads honour
    /// `max_local_read_bandwidth`.
    ThrottlerPtr local_throttler;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
    /// Per-query budget holder. Keeps the `FileCacheQueryLimit::QueryContext`
    /// registered under `query_id` for the lifetime of the provider, so
    /// `tryReserve` charges every `CacheWriter::write` against the same
    /// per-query budget — mirrors `CachedOnDiskReadBufferFromFile`'s holder.
    FileCache::QueryContextHolderPtr query_context_holder;
    /// Keep-alive anchors for recently-used cache-segment readers, borrowed by
    /// each `DiskCacheReader`. Keeps hot segment fds warm so the per-read
    /// `createReadBufferFromFileBase` hits `OpenedFileCache` instead of
    /// re-`open`ing. See `ReaderAnchorCache`.
    ReaderAnchorCache reader_anchors;
    /// One held cache-segment reader reused across windows by the sequential
    /// read path; borrowed by each `DiskCacheReader`. See `StreamingReaderSlot`.
    StreamingReaderSlot streaming_slot;
    LoggerPtr log = getLogger("DiskCacheProvider");
};

}
