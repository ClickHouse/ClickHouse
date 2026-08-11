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

/// A bounded keep-alive cache of open cache-segment readers, one per path. It holds each reader only
/// as an ANCHOR; it never reads through it. Keeping a reader alive keeps its `OpenedFile` warm, so
/// the next open is an `OpenedFileCache` hit. Internally synchronized.
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// A `CacheReader` over one hit range, backed by a read-only `FileSegmentsHolder` shared by all hit
/// buffers of the view. The holder keeps the segments pinned for the view's lifetime.
class DiskCacheReader : public CacheReader
{
public:
    DiskCacheReader(
        std::shared_ptr<FileSegmentsHolder> holder_,
        ByteRange range_in_file,
        size_t object_file_offset_,
        ThrottlerPtr local_throttler_,
        ReaderAnchorCache * anchors_);
    ~DiskCacheReader() override;

    ByteRange range() const override { return hit_range; }
    ChainedBuffers read(ByteRange subrange) override;

private:
    std::shared_ptr<FileSegmentsHolder> holder;
    ByteRange hit_range;
    size_t object_file_offset;
    ThrottlerPtr local_throttler;
    ReaderAnchorCache * anchors = nullptr;
    /// File-level sub-ranges this reader served; the destructor bumps their LRU priority.
    VectorWithMemoryTracking<ByteRange> hits_to_touch;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// A `CacheWriter` over one cache-aligned miss range. It owns its OWN `FileSegmentsHolder`, which the
/// `getOrSet` transaction in `resolve` builds. It appends across windows and finalizes at
/// destruction; the holder's destructor shrinks a partial segment to its downloaded size.
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
    ChainedBuffers read(ByteRange subrange) override;
    FillClaim claim(ByteRange window) override;
    ChainedBuffers waitAndRead(ByteRange subrange) override;

private:
    bool tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset);

    FileCachePtr cache;
    size_t object_file_offset;
    FilesystemCacheSettings cache_settings;
    /// SHARED with the other writers from the same `resolve`. Each writer's `aligned_range` selects
    /// its own segments from the holder.
    std::shared_ptr<FileSegmentsHolder> holder;
    IntervalSet committed_ranges;
    /// Guards `committed_ranges` only. The FileCache downloader gives per-segment write exclusion. A
    /// background prefetch and the foreground read append disjoint segments of the SAME writer at the
    /// same time, so both update this set.
    mutable std::mutex committed_mutex;
    ByteRange aligned_range;
    LoggerPtr log = getLogger("DiskCacheWriter");
};


/// An `ICacheProvider` that wraps a `FileCache`. Safe for concurrent use; parallel `readBigAt` shares
/// one provider. A lookup reads only immutable members and the internally-locked `FileCache`. The one
/// piece of shared mutable state (`ReaderAnchorCache`) is internally synchronized.
///
/// Cache key: `custom_cache_key`, else `FileCacheKey::fromPath`. Origin: `custom_origin`, else the
/// per-object `Data`/`System` classification.
class DiskCacheProvider : public ICacheProvider
{
public:
    /// `query_id` enforces `filesystem_cache_max_download_size`. The provider keeps a
    /// `QueryContextHolder` alive so `tryReserve` (inside `CacheWriter::write`) finds the per-query
    /// budget. An empty `query_id` means no per-query limit.
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

    /// Resolve `range` into hits (readers) and misses (writers when the tier populates). See the
    /// definition for the get / getOrSet split.
    VectorWithMemoryTracking<Resolution> resolve(
        const StoredObject & object, size_t object_file_offset, ByteRange range) override;

private:
    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    /// Forwarded to each `DiskCacheReader` to honour `max_local_read_bandwidth`.
    ThrottlerPtr local_throttler;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
    /// Keeps the per-query budget context alive; see the constructor.
    FileCache::QueryContextHolderPtr query_context_holder;
    /// Keep-alive anchors for recently-used cache-segment readers; see
    /// `ReaderAnchorCache`.
    ReaderAnchorCache reader_anchors;
    LoggerPtr log = getLogger("DiskCacheProvider");
};

}
