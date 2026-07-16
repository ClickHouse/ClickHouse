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

/// Keep-alive anchors for recently-used cache-segment readers, one per segment path: holding a
/// reader keeps its `OpenedFile` alive, so the next open is an `OpenedFileCache` hit (no syscall).
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// One held cache-segment reader reused across sequential windows. Never used by two threads at
/// once (the concurrent fan-out just degrades to fresh readers).
struct StreamingReaderSlot
{
    /// Take the held reader only when free, for `p`, and already at `offset`: a reused reader must
    /// not be `seek`-ed (it was last driven in external-buffer mode, whose stale coordinates
    /// mis-position the in-buffer seek shortcut).
    std::shared_ptr<ReadBufferFromFileBase> tryCheckout(const String & p, size_t offset);

    void checkin(const String & p, std::shared_ptr<ReadBufferFromFileBase> r, size_t next_pos);

    /// Never reuse a faulted reader (e.g. a read threw).
    void abandon();

    std::mutex mutex;
    String path;
    std::shared_ptr<ReadBufferFromFileBase> reader;
    size_t next_position = 0;
    bool checked_out = false;
};

/// `CacheReader` over one resident range, backed by a read-only `FileSegmentsHolder` shared by the
/// view's hit buffers (pins the segments for the view's lifetime).
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
    ChainedBuffers read(ByteRange sub) override;

private:
    std::shared_ptr<FileSegmentsHolder> holder;
    ByteRange hit_range;
    size_t object_file_offset;
    ThrottlerPtr local_throttler;
    ReaderAnchorCache * anchors = nullptr;
    StreamingReaderSlot * stream_slot = nullptr;
    /// The owning view's deferred-LRU-bump list; not owned (the view outlives this reader).
    VectorWithMemoryTracking<ByteRange> * hits_to_touch_sink = nullptr;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// `CacheWriter` over one cache-aligned miss range. Owns its `FileSegmentsHolder`, appends across
/// windows, and is finalized at destruction (the holder shrinks a partial segment to its size).
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
    IntervalSet committed() const override
    {
        std::lock_guard lock(committed_mutex);
        return committed_ranges;
    }
    size_t write(ChainedBuffers data) override;
    ChainedBuffers read(ByteRange sub) override;
    FillClaim claim(ByteRange window) override;
    ChainedBuffers waitAndReadSiblingLed(ByteRange sub) override;
    CacheWriter::CacheSegmentPin pin(size_t frontier) const override;

private:
    bool tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset);

    FileCachePtr cache;
    size_t object_file_offset;
    FilesystemCacheSettings cache_settings;
    FileSegmentsHolderPtr holder;
    IntervalSet committed_ranges;
    /// The worker and the foreground can append disjoint segments of this writer concurrently, so
    /// this guards `committed_ranges` (per-segment write exclusion is the FileCache downloader).
    mutable std::mutex committed_mutex;
    ByteRange aligned_range;
    LoggerPtr log = getLogger("DiskCacheWriter");
};

/// `CacheView` from `DiskCacheProvider::planResidencyView`; its destructor runs the deferred LRU
/// bump over the ranges the read buffers recorded.
class DiskCacheView : public CacheView
{
public:
    DiskCacheView(
        std::shared_ptr<FileSegmentsHolder> read_holder_,
        size_t object_file_offset_);

    ~DiskCacheView() override;

    /// Appended by the read buffers' `read` calls; consumed by the dtor's LRU bump.
    VectorWithMemoryTracking<ByteRange> hits_to_touch;

private:
    std::shared_ptr<FileSegmentsHolder> read_holder;
    size_t object_file_offset;
    LoggerPtr log = getLogger("DiskCacheView");
};


/// `ICacheProvider` wrapping FileCache; safe to share across the concurrent fan-out (its mutable
/// state -- `ReaderAnchorCache`, `StreamingReaderSlot`, `FileCache` -- is internally synchronized).
/// Cache key: `custom_cache_key` if set, else `FileCacheKey::fromPath(object.remote_path)`.
class DiskCacheProvider : public ICacheProvider
{
public:
    /// `query_id` (empty = no per-query limit) keeps a `QueryContextHolder` alive so `tryReserve`
    /// in `write` finds the per-query `filesystem_cache_max_download_size` budget.
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

    /// One `cache->get` (no creation): resident sub-ranges become hits, gaps become writer-null miss
    /// cells tiled on the boundary grid so a cut never falls inside an existing segment.
    CacheViewPtr planResidencyView(
        const StoredObject & object, size_t object_file_offset, ByteRange range_in_file) override;

    /// One `getOrSet` per surviving miss cell; the holder is owned by each writer.
    void openWriteBuffers(
        const StoredObject & object, size_t object_file_offset, CacheView & view) override;

private:
    /// The cache boundary grid (the quantum of segment starts/extents).
    size_t resolvedBoundaryAlignment() const;
    /// The extent virgin miss runs are tiled into: the S3-optimal request size, clamped to the
    /// cache's max segment size and kept a multiple of the boundary grid.
    size_t optimalFillCell() const;

    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    ThrottlerPtr local_throttler;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
    FileCache::QueryContextHolderPtr query_context_holder;
    ReaderAnchorCache reader_anchors;
    StreamingReaderSlot streaming_slot;
    LoggerPtr log = getLogger("DiskCacheProvider");
};

}
