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

/// Shared deferred-LRU-bump context of one probe: every hit reader records the
/// sub-ranges it served, and the LAST owner's destruction runs the bump over
/// the pinned holder - the probe cursor while the walk runs, then the final
/// handed-out reader (the slide's release point).
struct DiskCacheTouchBook
{
    DiskCacheTouchBook(std::shared_ptr<FileSegmentsHolder> holder_, size_t object_file_offset_)
        : holder(std::move(holder_)), object_file_offset(object_file_offset_)
    {
    }
    ~DiskCacheTouchBook();

    std::shared_ptr<FileSegmentsHolder> holder;
    size_t object_file_offset;
    VectorWithMemoryTracking<ByteRange> touched;
    LoggerPtr log = getLogger("DiskCacheTouchBook");
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
        std::shared_ptr<DiskCacheTouchBook> touch_book_);

    ByteRange range() const override { return hit_range; }
    ChainedBuffers read(ByteRange sub) override;

private:
    std::shared_ptr<FileSegmentsHolder> holder;
    ByteRange hit_range;
    size_t object_file_offset;
    ThrottlerPtr local_throttler;
    ReaderAnchorCache * anchors = nullptr;
    StreamingReaderSlot * stream_slot = nullptr;
    /// The probe's shared deferred-bump book: each `read` records its `sub`
    /// here; the bump runs when the book's last owner dies.
    std::shared_ptr<DiskCacheTouchBook> touch_book;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// `CacheWriter` over one cache-aligned miss range. Owns its OWN
/// `FileSegmentsHolder` (the `getOrSet` transaction in `resolve` builds it),
/// appends across windows and is finalized at destruction - the holder's
/// destructor shrinks a partial segment to its downloaded size.
class DiskCacheWriter : public CacheWriter
{
public:
    DiskCacheWriter(
        FileCachePtr cache_,
        size_t object_file_offset_,
        const FilesystemCacheSettings & cache_settings_,
        std::shared_ptr<FileSegmentsHolder> holder_,
        ByteRange aligned_range_in_file);

    ByteRange range() const override { return aligned_range; }
    IntervalSet committed() const override
    {
        std::lock_guard lock(committed_mutex);
        return committed_ranges;
    }
    size_t write(ChainedBuffers data) override;
    ChainedBuffers read(ByteRange sub) override;
    FillClaim claim(ByteRange window) override;
    ChainedBuffers waitAndReadSiblingLed(ByteRange sub) override;
    bool frontierInPartial(size_t frontier) const override;

private:
    bool tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset);

    FileCachePtr cache;
    size_t object_file_offset;
    FilesystemCacheSettings cache_settings;
    /// SHARED with sibling writers born of the same ranged `lookAt` - each
    /// writer's `aligned_range` selects its own segment(s) from the holder.
    std::shared_ptr<FileSegmentsHolder> holder;
    IntervalSet committed_ranges;
    /// Guards `committed_ranges` only. Per-segment write exclusion is the FileCache
    /// downloader (`getOrSetDownloader`), but the worker and the foreground can append
    /// disjoint segments of the SAME writer concurrently, racing this `IntervalSet`.
    mutable std::mutex committed_mutex;
    ByteRange aligned_range;
    LoggerPtr log = getLogger("DiskCacheWriter");
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

    /// Resolve `range` into hits (readers) and misses (writers when
    /// populating); see the definition for the get/getOrSet split.
    VectorWithMemoryTracking<Resolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) override;

private:

    /// The cache boundary grid: the quantum of segment starts/extents (as
    /// `getOrSet` resolves it).
    size_t resolvedBoundaryAlignment() const;
    /// The tile unit for miss runs: the cache's own maximum segment size,
    /// floored to a multiple of the boundary grid - demand-shaped tiling makes
    /// the biggest segments the cache allows where the demand is known to run
    /// through, and the demand edge tapers the last cell to the grid.
    size_t maxFillCell() const;

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
