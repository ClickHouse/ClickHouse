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
///   1. Source             -- base ReadBuffer (S3, Azure, HDFS, local file)
///   2. FilesystemCache    -- CachedOnDiskReadBufferFromFile
///   3. Gather             -- ReadBufferFromRemoteFSGather (multi-object files)
///   4. DistributedCache   -- ReadBufferFromDistributedCache (with fallback to Gather)
///   5. MemoryCache        -- CachedInMemoryReadBufferFromFile
///   6. AsyncPrefetch      -- AsynchronousBoundedReadBuffer
///   7. Encryption         -- ReadBufferFromEncryptedFile (may have multiple layers)
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
    /// Each setter takes ReadSettings which are stored alongside the source
    /// and used by build() for buffer sizing and readObject() calls.

    /// Set source from an object storage.
    void setSource(ObjectStoragePtr storage, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint = {});

    /// Set source from a local file path.
    void setLocalFileSource(String path, StoredObjects objects, const ReadSettings & read_settings, std::optional<size_t> read_hint = {});

    /// Set source from a backup.
    void setBackupSource(std::shared_ptr<IBackup> backup, String path, StoredObjects objects, const ReadSettings & read_settings);

    /// Set source with a custom buffer creator (for testing or custom backends).
    void setSource(BufferCreator creator, StoredObjects objects, const ReadSettings & read_settings);

    /// -- Gather stage (ReadBufferFromRemoteFSGather) --
    /// Joins multiple stored objects into a single seekable buffer.
    /// Required for object storage where one logical file maps to multiple blobs.
    /// Not needed for local disk where one file = one file.
    void needGather();

    /// -- Filesystem cache stage --
    void needFilesystemCache(FileCachePtr cache, FilesystemCacheSettings cache_settings, std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);

    /// Overload with a custom cache key and origin, bypassing the default `FileCacheKey::fromPath` derivation.
    /// Used by `StorageObjectStorageSource` where the cache key is `SipHash(path + etag)`.
    void needFilesystemCache(
        FileCachePtr cache,
        FileCacheKey cache_key,
        FileCacheOriginInfo origin,
        FilesystemCacheSettings cache_settings,
        std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);

    /// -- Memory cache stage --
    /// The cache pointer travels inside `page_cache_settings.cache`; a null cache disables the stage.
    void needMemoryCache(String cache_path_prefix, PageCacheSettings page_cache_settings);

    /// Overload with a fully custom page cache key (path + file_version), bypassing the default
    /// `cache_path_prefix + object.remote_path` derivation.
    /// Used by `StorageObjectStorageSource` where the key is `"s3:" + path` with `"etag:" + etag`.
    void needMemoryCache(
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

    /// -- Decryption stage --
    /// The key_finder callback is called at build time with the key fingerprint
    /// read from the encryption header. It must return the decryption key.
    void needDecryption(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// -- Build the final ReadBuffer chain --
    /// Uses the ReadSettings stored in the source stage.
    std::unique_ptr<ReadBufferFromFileBase> build() const;

    /// Returns a human-readable description of active stages,
    /// e.g. "Source -> FilesystemCache -> Gather -> Async".
    String describe() const;

    /// Creates a copy of this pipeline (all stages are preserved).
    ReadPipeline clone() const;

    /// Queries.
    bool hasSource() const { return source.has_value(); }
    const StoredObjects & getStoredObjects() const;

private:
    struct SourceStage
    {
        StoredObjects objects;
        std::variant<ObjectStorageSource, LocalFileSource, BackupSource, CustomSource> source;
        ReadSettings read_settings;
    };

    struct FilesystemCacheStage
    {
        FileCachePtr cache;
        std::shared_ptr<FilesystemCacheLog> cache_log;
        FilesystemCacheSettings cache_settings;
        std::optional<FileCacheKey> custom_cache_key;       /// Override per-object cache key
        std::optional<FileCacheOriginInfo> custom_origin;   /// Override origin
    };

    struct MemoryCacheStage
    {
        String cache_path_prefix;
        PageCacheSettings page_cache_settings;          /// Carries the `cache` shared_ptr
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
    std::vector<FilesystemCacheStage> filesystem_caches;
    std::optional<MemoryCacheStage> memory_cache;
    std::optional<DistributedCacheStage> distributed_cache;
    std::optional<AsyncPrefetchStage> async_prefetch;
    std::vector<DecryptionStage> decryption_stages;

    /// build() helpers: one per logical stage group.
    /// Each helper reads private state and returns the (partial) impl buffer.
    /// `query_id` is captured once on the calling thread before any stage runs.
    std::unique_ptr<ReadBufferFromFileBase> buildGatherStage(const std::string & query_id) const;
    std::unique_ptr<ReadBufferFromFileBase> buildSingleObjectStage(const std::string & query_id) const;
    std::unique_ptr<ReadBufferFromFileBase> wrapMemoryCache(std::unique_ptr<ReadBufferFromFileBase> impl) const;
    std::unique_ptr<ReadBufferFromFileBase> wrapAsyncPrefetch(std::unique_ptr<ReadBufferFromFileBase> impl) const;
    std::unique_ptr<ReadBufferFromFileBase> wrapDecryption(std::unique_ptr<ReadBufferFromFileBase> impl) const;
};

}
