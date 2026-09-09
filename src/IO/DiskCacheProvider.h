#pragma once

#include <IO/ICacheProvider.h>
#include <IO/IntervalSet.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/CacheBase.h>
#include <IO/ReadBufferFromFileBase.h>


namespace DB
{

/// A bounded keep-alive cache of open cache-segment readers, one per path. It holds each reader only
/// as an ANCHOR; it never reads through it. Keeping a reader alive keeps its `OpenedFile` warm, so
/// the next open is an `OpenedFileCache` hit. Internally synchronized.
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// A `CacheReader` over one hit range, backed by a single-segment `FileSegmentsHolder`. The holder
/// completes the segment on destruction (a read-only hit is a no-op).
class DiskCacheReader : public CacheReader
{
public:
    DiskCacheReader(
        FileSegmentsHolderSharedPtr segment_holder_,
        ByteRange range_in_file,
        size_t object_file_offset_,
        ThrottlerPtr local_throttler_,
        ReaderAnchorCache * anchors_);
    ~DiskCacheReader() override;

    ByteRange range() const override { return hit_range; }
    ChainedBuffers read(ByteRange subrange) override;

private:
    /// Our holder always carries exactly one segment (see the constructor); assert it on each access.
    FileSegment & segment() const { chassert(segment_holder && segment_holder->size() == 1); return segment_holder->front(); }

    FileSegmentsHolderSharedPtr segment_holder;
    ByteRange hit_range;
    /// The blob's start offset in the logical file (multi-blob files); cache coordinates are blob-local.
    size_t object_file_offset;
    ThrottlerPtr local_throttler;
    ReaderAnchorCache * anchors = nullptr;
    /// Whether this reader served any bytes; the destructor then bumps the segment's cache priority.
    bool served = false;
    LoggerPtr log = getLogger("DiskCacheReader");
};

/// A `CacheWriter` over one cache-aligned miss segment, held via a single-segment `FileSegmentsHolder`
/// (shared with the segment's hit reader for a partial). Appends across windows; the holder completes
/// the segment on destruction, shrinking a partial to its downloaded size.
class DiskCacheWriter : public CacheWriter
{
public:
    DiskCacheWriter(
        FileCachePtr cache_,
        size_t object_file_offset_,
        const FilesystemCacheSettings & cache_settings_,
        FileSegmentsHolderSharedPtr segment_holder_,
        ByteRange aligned_range_in_file);

    ByteRange range() const override { return aligned_range; }
    size_t committed() const override;
    size_t write(ChainedBuffers data, const FillRole & role) override;
    ChainedBuffers read(ByteRange subrange) override;
    FillRole takeFillRole() override;
    ChainedBuffers waitAndRead(ByteRange subrange) override;

private:
    bool tryWriteToSegment(FileSegment & file_segment, char * data, size_t size, size_t offset);

    /// Our holder always carries exactly one segment (see the constructor); assert it on each access.
    FileSegment & segment() const { chassert(segment_holder && segment_holder->size() == 1); return segment_holder->front(); }

    FileCachePtr cache;
    /// The blob's start offset in the logical file (multi-blob files); cache coordinates are blob-local.
    size_t object_file_offset;
    FilesystemCacheSettings cache_settings;
    FileSegmentsHolderSharedPtr segment_holder;
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

    /// Resolve `range` into hits (readers) and misses (writers when the tier populates). See the
    /// definition for the get / getOrSet split.
    VectorWithMemoryTracking<CacheResolution> resolve(
        const StoredObject & object, size_t object_offset, ByteRange range) override;

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
