#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Interpreters/FileCache/FileCacheKey.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <IO/ReadSettings.h>

#include <functional>
#include <memory>
#include <optional>
#include <variant>

namespace DB
{

class ReadBufferFromFileBase;
class FileCache;
class FilesystemCacheLog;
class FilesystemReadPrefetchesLog;
class PageCache;
class IAsynchronousReader;
class IBackup;
struct AsyncReadCounters;

using FileCachePtr = std::shared_ptr<FileCache>;
using AsyncReadCountersPtr = std::shared_ptr<AsyncReadCounters>;
using FilesystemReadPrefetchesLogPtr = std::shared_ptr<FilesystemReadPrefetchesLog>;

/// ReadPipeline: a declarative specification for creating a read buffer chain.
///
/// Instead of imperatively nesting read buffers (the "matryoshka" pattern),
/// subsystems annotate a ReadPipeline with their requirements (stages).
/// The `build` method assembles the actual buffer chain in a fixed order.
///
/// Usage:
///   ReadPipeline pipeline;
///   disk->prepareRead(path, settings, read_hint, pipeline);  // sets source, settings, stages
///   auto buf = pipeline.build();
///
/// Stage ordering (innermost to outermost, fixed at build time):
///   1. Source        -- base ReadBuffer (S3, Azure, HDFS, local file)
///   2. DiskCache     -- CachedOnDiskReadBufferFromFile
///   3. Gather        -- ReadBufferFromRemoteFSGather (auto for multi-object)
///   4. MemoryCache   -- CachedInMemoryReadBufferFromFile
///   5. AsyncPrefetch -- AsynchronousBoundedReadBuffer
///   6. Decrypt       -- ReadBufferFromEncryptedFile
///   7. Decompress    -- CompressedReadBufferFromFile
class ReadPipeline
{
public:
    /// Source descriptor types — store WHAT to read, not HOW.

    /// Object storage source (S3, Azure, HDFS, etc.)
    struct ObjectStorageSource
    {
        ObjectStoragePtr storage;
        std::optional<size_t> read_hint;
    };

    /// Local filesystem source.
    struct LocalFileSource
    {
        String path;
        std::optional<size_t> read_hint;
    };

    /// Backup storage source.
    struct BackupSource
    {
        std::shared_ptr<IBackup> backup;
        String path;
    };

    /// Custom source for testing or special backends.
    using BufferCreator = std::function<std::unique_ptr<ReadBufferFromFileBase>(
        const StoredObject & object,
        const ReadSettings & settings,
        bool use_external_buffer,
        bool restrict_seek)>;

    struct CustomSource
    {
        BufferCreator creator;
    };

    /// Function that finds an encryption key by its fingerprint.
    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    ReadPipeline() = default;
    ReadPipeline(const ReadPipeline &) = default;
    ReadPipeline & operator=(const ReadPipeline &) = default;
    ReadPipeline(ReadPipeline &&) = default;
    ReadPipeline & operator=(ReadPipeline &&) = default;

    /// -- Source stage --

    /// Set source from an object storage.
    void setSource(ObjectStoragePtr storage, StoredObjects objects, std::optional<size_t> read_hint = {});

    /// Set source from a local file path.
    void setLocalFileSource(String path, StoredObjects objects, std::optional<size_t> read_hint = {});

    /// Set source from a backup.
    void setBackupSource(std::shared_ptr<IBackup> backup, String path, StoredObjects objects);

    /// Set source with a custom buffer creator (for testing or custom backends).
    void setSource(StoredObjects objects, BufferCreator creator);

    /// -- Gather stage (ReadBufferFromRemoteFSGather) --
    /// Joins multiple stored objects into a single seekable buffer.
    /// Required for object storage where one logical file maps to multiple blobs.
    /// Not needed for local disk where one file = one file.
    void needGather();

    /// -- Disk cache stage --
    void needDiskCache(FileCachePtr cache, std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);
    void needDiskCache(FileCachePtr cache, FilesystemCacheSettings cache_settings, std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);

    /// Overload with a custom cache key and origin, bypassing the default `FileCacheKey::fromPath` derivation.
    /// Used by `StorageObjectStorageSource` where the cache key is `SipHash(path + etag)`.
    void needDiskCache(
        FileCachePtr cache,
        FileCacheKey cache_key,
        FileCacheOriginInfo origin,
        FilesystemCacheSettings cache_settings,
        std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);

    /// -- Memory cache stage --
    void needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix);
    void needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix, PageCacheSettings page_cache_settings);

    /// Overload with a fully custom page cache key (path + file_version), bypassing the default
    /// `cache_path_prefix + object.remote_path` derivation.
    /// Used by `StorageObjectStorageSource` where the key is `"s3:" + path` with `"etag:" + etag`.
    void needMemoryCache(
        std::shared_ptr<PageCache> cache,
        String custom_cache_path,
        String custom_file_version,
        PageCacheSettings page_cache_settings);

    /// -- Distributed cache stage (sits between Gather and MemoryCache) --
    /// Implementation is in the DistributedCache module (ENABLE_DISTRIBUTED_CACHE).
    /// When enabled, reads go through the distributed cache with fallback to Gather.
    /// Also affects: use_page_cache condition and min_bytes_for_seek in AsyncPrefetch.
    /// @param include_credentials_in_cache_key  When true, object storage credentials are
    ///        included in the cache key hash. Set to true for table engine reads (s3(...), etc.)
    ///        where different users may access the same path with different credentials.
    void needDistributedCache(bool include_credentials_in_cache_key = false);

    /// -- Async prefetch stage --
    void needAsyncPrefetch(
        IAsynchronousReader & reader,
        AsyncReadCountersPtr async_read_counters = nullptr,
        FilesystemReadPrefetchesLogPtr prefetches_log = nullptr);

    /// Set the read settings used by build() to construct the buffer chain.
    /// Must be called before build(). Typically called by prepareRead() after
    /// applying disk-specific adjustments (e.g. IO scheduling).
    void setReadSettings(ReadSettings settings) { read_settings = std::move(settings); }

    /// -- Decryption stage --
    /// The key_finder callback is called at build time with the key fingerprint
    /// read from the encryption header. It must return the decryption key.
    void needDecryption(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// -- Build the final ReadBuffer chain --
    /// Uses the ReadSettings stored via setReadSettings().
    std::unique_ptr<ReadBufferFromFileBase> build() const;

    /// Returns a human-readable description of active stages,
    /// e.g. "Source -> DiskCache -> Gather -> Async".
    String describe() const;

    /// Creates a copy of this pipeline (all stages are preserved).
    ReadPipeline clone() const;

    /// Queries.
    bool hasSource() const { return source.has_value(); }
    bool hasReadSettings() const { return read_settings.has_value(); }
    const StoredObjects & getStoredObjects() const;

private:
    struct SourceStage
    {
        StoredObjects objects;
        std::variant<ObjectStorageSource, LocalFileSource, BackupSource, CustomSource> source;
    };

    struct DiskCacheStage
    {
        FileCachePtr cache;
        std::shared_ptr<FilesystemCacheLog> cache_log;
        std::optional<FilesystemCacheSettings> cache_settings;
        std::optional<FileCacheKey> custom_cache_key;       /// Override per-object cache key
        std::optional<FileCacheOriginInfo> custom_origin;   /// Override origin
    };

    struct MemoryCacheStage
    {
        std::shared_ptr<PageCache> cache;
        String cache_path_prefix;
        std::optional<PageCacheSettings> page_cache_settings;
        std::optional<String> custom_cache_path;        /// Override the full cache key path
        std::optional<String> custom_file_version;      /// Override the file_version in the cache key
    };

    struct AsyncPrefetchStage
    {
        IAsynchronousReader * reader = nullptr;
        AsyncReadCountersPtr async_read_counters;
        FilesystemReadPrefetchesLogPtr prefetches_log;
    };

    struct DecryptionStage
    {
        String path;
        size_t buffer_size;
        KeyFinderFunc key_finder;
    };


    struct DistributedCacheStage
    {
        bool include_credentials_in_cache_key = false;
    };

    std::optional<SourceStage> source;
    bool gather = false;
    std::optional<DiskCacheStage> disk_cache;
    std::optional<MemoryCacheStage> memory_cache;
    std::optional<DistributedCacheStage> distributed_cache;
    std::optional<AsyncPrefetchStage> async_prefetch;
    std::vector<DecryptionStage> decryption_stages;
    std::optional<ReadSettings> read_settings;
};

}
