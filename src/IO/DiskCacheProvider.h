#pragma once

#include <IO/ICacheProvider.h>
#include <IO/ReadSettings.h>
#include <Interpreters/FileCache/FileCache.h>

#include <Common/logger_useful.h>

namespace DB
{

class FilesystemCacheLog;

/// ICacheHandle for FileCache (filesystem/disk cache).
/// Holds a FileSegmentsHolder — segments stay pinned until the handle is destroyed.
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
        std::shared_ptr<FilesystemCacheLog> cache_log,
        String source_file_path);

    ~DiskCacheHandle() override;

    CacheLookupResult status() const override;
    Rope get(ByteRange range) override;
    size_t put(ByteRange range, Rope data) override;

private:
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
    std::shared_ptr<FilesystemCacheLog> cache_log;
    String source_file_path;
    ByteRange requested_range;
    FileSegmentsHolderPtr holder;
    /// File-level ranges returned by successful `get` calls. The destructor
    /// re-fetches the matching segments and calls `increasePriority` on each
    /// — the executor keeps the handle alive until after every `put` so the
    /// bump always lands AFTER the inserts. See `~DiskCacheHandle`.
    std::vector<ByteRange> hits_to_touch;
    LoggerPtr log = getLogger("DiskCacheHandle");
};


/// ICacheProvider wrapping FileCache.
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
    DiskCacheProvider(
        FileCachePtr cache_,
        const FilesystemCacheSettings & cache_settings_,
        std::shared_ptr<FilesystemCacheLog> cache_log_ = nullptr,
        std::optional<FileCacheKey> custom_cache_key_ = std::nullopt,
        std::optional<FileCacheOriginInfo> custom_origin_ = std::nullopt)
        : cache(std::move(cache_))
        , cache_settings(cache_settings_)
        , cache_log(std::move(cache_log_))
        , custom_cache_key(std::move(custom_cache_key_))
        , custom_origin(std::move(custom_origin_))
    {
    }

    std::unique_ptr<ICacheHandle> lookup(
        const StoredObject & object,
        size_t object_file_offset,
        ByteRange range_in_file) override;
    String name() const override { return "DiskCache"; }

private:
    FileCachePtr cache;
    FilesystemCacheSettings cache_settings;
    std::shared_ptr<FilesystemCacheLog> cache_log;
    std::optional<FileCacheKey> custom_cache_key;
    std::optional<FileCacheOriginInfo> custom_origin;
};

}
