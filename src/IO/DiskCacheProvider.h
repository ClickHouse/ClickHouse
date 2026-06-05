#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/CacheBase.h>
#include <IO/ReadBufferFromFileBase.h>

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
/// `DiskCacheHandle::get` reads through its own fresh reader — nothing to race on.
using ReaderAnchorCache = CacheBase<String, ReadBufferFromFileBase>;

/// Holds a `FileSegmentsHolder` for the request's lifetime so the segments
/// referenced by `status` / `get` stay pinned across the handle's calls.
class DiskCacheHandle : public ICacheHandle
{
public:
    DiskCacheHandle(
        FileCachePtr cache,
        FileCacheKey cache_key,
        FileCacheOriginInfo origin,
        size_t object_file_offset,
        size_t object_size,
        ByteRange requested,
        const FilesystemCacheSettings & cache_settings,
        ThrottlerPtr local_throttler,
        String source_file_path,
        ReaderAnchorCache * anchors);

    ~DiskCacheHandle() override;

    CacheLookupResult status() const override;
    Rope get(ByteRange range) override;
    size_t put(ByteRange range, Rope data) override;
    ICacheHandle::CacheSegmentPin pinSegmentAt(size_t file_offset) const override;

private:
    size_t writeToSegment(FileSegment & segment, ByteRange range_in_object, const Rope & data);
    bool tryWriteToSegment(FileSegment & segment, char * data, size_t size, size_t offset);

    FileCachePtr cache;
    FileCacheKey cache_key;
    FileCacheOriginInfo origin;
    /// Where this object starts inside the file the executor is reading.
    /// All public-facing `ByteRange`s on this handle are file-level; we
    /// subtract `object_file_offset` to obtain the object-local offsets
    /// that `FileCache` keys by.
    size_t object_file_offset;
    /// Size of the object itself (bytes_size from StoredObject).
    size_t object_size;
    FilesystemCacheSettings cache_settings;
    /// Pipeline's local-read throttler, propagated into the per-segment
    /// `createReadBufferFromFileBase` call in `get`. Carrying just the
    /// throttler keeps the contract narrow — the cache-file `ReadSettings`
    /// `get()` constructs is otherwise fully fixed (pread method,
    /// external-buffer mode), so there's nothing else worth forwarding
    /// from the caller's `ReadSettings`.
    ThrottlerPtr local_throttler;
    String source_file_path;
    /// Borrowed from the owning `DiskCacheProvider` (which outlives this
    /// per-call handle): the keep-alive anchor cache that `get` inserts each
    /// just-used reader into. Not owned; null disables anchoring.
    ReaderAnchorCache * anchors = nullptr;
    ByteRange requested_range;
    FileSegmentsHolderPtr holder;
    /// File-level ranges returned by successful `get` calls. The destructor
    /// re-fetches the matching segments and calls `increasePriority` on each
    /// — the executor keeps the handle alive until after every `put` so the
    /// bump always lands AFTER the inserts. See `~DiskCacheHandle`.
    VectorWithMemoryTracking<ByteRange> hits_to_touch;
    LoggerPtr log = getLogger("DiskCacheHandle");
};


/// ICacheProvider wrapping FileCache.
///
/// Safe for concurrent use: `lookup` only reads immutable members and the
/// internally-locked `FileCache`, each `DiskCacheHandle` is per-call, and the
/// only shared mutable state — the `ReaderAnchorCache` keep-alive set — is
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
    /// `DiskCacheHandle::put`) finds the matching per-query budget when it
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

    std::unique_ptr<ICacheHandle> lookup(
        const StoredObject & object,
        size_t object_file_offset,
        ByteRange range_in_file) override;
    String name() const override { return "DiskCache"; }
    CacheTier tier() const override { return CacheTier::FilesystemCache; }

private:
    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    /// Forwarded into each `DiskCacheHandle` so cache-file reads in `get`
    /// honour `max_local_read_bandwidth`.
    ThrottlerPtr local_throttler;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
    /// Per-query budget holder. Keeps the `FileCacheQueryLimit::QueryContext`
    /// registered under `query_id` for the lifetime of the provider, so
    /// `tryReserve` charges every `DiskCacheHandle::put` against the same
    /// per-query budget — mirrors `CachedOnDiskReadBufferFromFile`'s holder.
    FileCache::QueryContextHolderPtr query_context_holder;
    /// Keep-alive anchors for recently-used cache-segment readers, borrowed by
    /// each handle in `lookup`. Keeps hot segment fds warm so the per-call
    /// `createReadBufferFromFileBase` in `get` hits `OpenedFileCache` instead of
    /// re-`open`ing. See `ReaderAnchorCache`.
    ReaderAnchorCache reader_anchors;
};

}
