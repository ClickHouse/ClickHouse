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
/// segment path. The readers are held only as ANCHORS, never read through:
/// keeping a recently-used reader alive keeps its `OpenedFile` alive, so the
/// next `createReadBufferFromFileBase` is an `OpenedFileCache` hit (no `open`
/// syscall). `CacheBase` is internally synchronized.
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// One held cache-segment reader, reused across windows by the sequential read
/// path. A caller either takes the held reader (exclusive until check-in) or
/// opens its own fresh one - the reader is never used by two threads at once,
/// which is what makes this safe under the concurrent `readBigAt` fan-out
/// (the slot just degrades to fresh readers there).
struct StreamingReaderSlot
{
    /// Take the held reader ONLY when it is free, for `p`, AND already sitting
    /// at `offset` - the next read must be exactly contiguous, because a reused
    /// reader must never be `seek`-ed: it was last driven in external-buffer
    /// (`set`) mode, whose stale working-buffer coordinates make `seek`'s
    /// in-buffer shortcut mis-position.
    std::shared_ptr<ReadBufferFromFileBase> tryCheckout(const String & p, size_t offset);

    /// Return `r` as the held reader for `p`, positioned at `next_pos`, free.
    void checkin(const String & p, std::shared_ptr<ReadBufferFromFileBase> r, size_t next_pos);

    /// Drop the held reader (e.g. a read threw): never reuse a faulted reader.
    void abandon();

    std::mutex mutex;
    String path;
    std::shared_ptr<ReadBufferFromFileBase> reader;
    /// File offset the held reader is positioned at.
    size_t next_position = 0;
    bool checked_out = false;
};

/// `CacheReader` over one resident range, backed by a read-only
/// `FileSegmentsHolder` shared by all hit buffers of the view (keeps the
/// segments pinned for the view's lifetime).
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
        VectorWithMemoryTracking<ByteRange> * hits_to_touch_sink_);

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
    /// The owning view's deferred-bump list: each `read` records its `sub`
    /// here for the LRU bump in `~DiskCacheView`. Not owned; the view outlives
    /// this buffer.
    VectorWithMemoryTracking<ByteRange> * hits_to_touch_sink = nullptr;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// `CacheWriter` over one cache-aligned miss range. Owns its OWN
/// `FileSegmentsHolder` (one `getOrSet`, built by `openWriteBuffers`), appends
/// across windows and is finalized at destruction - the holder's destructor
/// shrinks a partial segment to its downloaded size.
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

/// `CacheView` from `DiskCacheProvider::planResidencyView`. Holds the shared
/// read-only holder plus the deferred-LRU-bump context; its destructor runs
/// the bump over all ranges the read buffers recorded. Misses carry
/// `writer == nullptr`.
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

    const VectorWithMemoryTracking<HitEntry> & hits() const override { return hit_entries; }
    const VectorWithMemoryTracking<MissEntry> & misses() const override { return miss_entries; }

    VectorWithMemoryTracking<HitEntry> hit_entries;
    VectorWithMemoryTracking<MissEntry> miss_entries;
    /// Appended to by the owned read buffers' `read` calls; consumed by the dtor.
    VectorWithMemoryTracking<ByteRange> hits_to_touch;

private:
    std::shared_ptr<FileSegmentsHolder> read_holder;
    FileCachePtr cache;
    FileCacheKey cache_key;
    FileCacheOriginInfo origin;
    size_t object_file_offset;
    LoggerPtr log = getLogger("DiskCacheView");
};


/// `ICacheProvider` wrapping FileCache. Safe for concurrent use (the
/// `readBigAt` fan-out shares one provider): lookups only read immutable
/// members and the internally-locked `FileCache`; the shared mutable state
/// (`ReaderAnchorCache`, `StreamingReaderSlot`) is internally synchronized.
///
/// Cache identity per object: `custom_cache_key` when set (single-object,
/// etag-keyed flow), else `FileCacheKey::fromPath(object.remote_path)`
/// (multi-object gather mode). Origin: `custom_origin` when set, else the
/// per-object `Data`/`System` classification by file extension.
class DiskCacheProvider : public ICacheProvider
{
public:
    /// `query_id` enforces `filesystem_cache_max_download_size`: the provider
    /// keeps a `QueryContextHolder` alive so `tryReserve` (inside
    /// `CacheWriter::write`) finds the per-query budget. Empty `query_id`
    /// means no per-query limit.
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

    /// A miss segment is created at the `boundary_alignment` floor and its
    /// write buffer appends from that floor, so the fetch head must reach it.
    /// The tail fills incrementally - no tail rounding.
    size_t fetchHeadAlignment() const override { return cache->getBoundaryAlignment(); }

    /// One `cache->get` (no segment creation): each resident sub-range becomes
    /// a `HitEntry`, each gap a cache-aligned writer-null `MissEntry`. A
    /// concurrently-DOWNLOADING segment credits its committed prefix as a hit
    /// and misses only the tail.
    CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override;

    /// One `getOrSet` per range; the held holder is owned by each writer.
    VectorWithMemoryTracking<MissEntry> openWriteBuffers(
        const StoredObject & object, size_t object_file_offset,
        const VectorWithMemoryTracking<ByteRange> & aligned_miss_ranges) override;

private:
    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    /// Forwarded into each `DiskCacheReader` so cache-file reads honour
    /// `max_local_read_bandwidth`.
    ThrottlerPtr local_throttler;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
    /// Keeps the per-query budget context registered for the provider's
    /// lifetime (see the constructor doc).
    FileCache::QueryContextHolderPtr query_context_holder;
    /// Keep-alive anchors for recently-used cache-segment readers; see
    /// `ReaderAnchorCache`.
    ReaderAnchorCache reader_anchors;
    /// See `StreamingReaderSlot`.
    StreamingReaderSlot streaming_slot;
    LoggerPtr log = getLogger("DiskCacheProvider");
};

}
