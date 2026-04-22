#pragma once

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadSettings.h>

#include <functional>
#include <memory>
#include <optional>

namespace DB
{

class ReadBufferFromFileBase;
class FileCache;
class FilesystemCacheLog;
class FilesystemReadPrefetchesLog;
class PageCache;
class IAsynchronousReader;
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
///   objectStorage->prepareRead(path, pipeline);     // sets source
///   cachedStorage->prepareRead(path, pipeline);     // adds disk cache
///   diskObjectStorage->prepareRead(path, pipeline); // adds async, memory cache
///   diskEncrypted->prepareRead(path, pipeline);     // adds decryption
///   auto buf = pipeline.build(read_settings);
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
    /// Function that creates a ReadBuffer for a single stored object.
    using BufferCreator = std::function<std::unique_ptr<ReadBufferFromFileBase>(
        const StoredObject & object,
        const ReadSettings & settings)>;

    /// Function that finds an encryption key by its fingerprint.
    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    ReadPipeline() = default;
    ReadPipeline(const ReadPipeline &) = default;
    ReadPipeline & operator=(const ReadPipeline &) = default;
    ReadPipeline(ReadPipeline &&) = default;
    ReadPipeline & operator=(ReadPipeline &&) = default;

    /// -- Source stage --

    /// Set source from an object storage (creates a BufferCreator internally).
    void setSource(ObjectStoragePtr storage, StoredObjects objects, std::optional<size_t> read_hint = {});

    /// Set source with a custom buffer creator (useful for testing or custom backends).
    void setSource(StoredObjects objects, BufferCreator creator);

    /// -- Gather stage (ReadBufferFromRemoteFSGather) --
    /// Joins multiple stored objects into a single seekable buffer.
    /// Required for object storage where one logical file maps to multiple blobs.
    /// Not needed for local disk where one file = one file.
    void needGather();

    /// -- Disk cache stage --
    void needDiskCache(FileCachePtr cache, std::shared_ptr<FilesystemCacheLog> cache_log = nullptr);

    /// -- Memory cache stage --
    void needMemoryCache(std::shared_ptr<PageCache> cache, String cache_path_prefix);

    /// -- Distributed cache stage (sits between Gather and MemoryCache) --
    /// Implementation is in the DistributedCache module (ENABLE_DISTRIBUTED_CACHE).
    /// When enabled, reads go through the distributed cache with fallback to Gather.
    /// Also affects: use_page_cache condition and min_bytes_for_seek in AsyncPrefetch.
    void needDistributedCache();

    /// -- Async prefetch stage --
    void needAsyncPrefetch(
        IAsynchronousReader & reader,
        AsyncReadCountersPtr async_read_counters = nullptr,
        FilesystemReadPrefetchesLogPtr prefetches_log = nullptr);

    /// Override the buffer size used by gather and async prefetch in build.
    /// Used by DiskObjectStorage to implement prefer_bigger_buffer_size.
    void setBufferSize(size_t size) { buffer_size_override = size; }

    /// Override settings used by gather (e.g. with IO scheduling resource links).
    /// build uses these for the gather constructor instead of the caller's settings.
    void setGatherSettings(ReadSettings settings) { gather_settings_override = std::move(settings); }

    /// -- Decryption stage --
    /// The key_finder callback is called at build time with the key fingerprint
    /// read from the encryption header. It must return the decryption key.
    void needDecryption(String path, size_t buffer_size, KeyFinderFunc key_finder);

    /// -- Decompression stage --
    void needDecompression(bool allow_different_codecs = false);

    /// -- Build the final ReadBuffer chain --
    std::unique_ptr<ReadBufferFromFileBase> build(const ReadSettings & settings) const;

    /// Returns a human-readable description of active stages,
    /// e.g. "Source -> DiskCache -> Gather -> Async".
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
        BufferCreator creator;
    };

    struct DiskCacheStage
    {
        FileCachePtr cache;
        std::shared_ptr<FilesystemCacheLog> cache_log;
    };

    struct MemoryCacheStage
    {
        std::shared_ptr<PageCache> cache;
        String cache_path_prefix;
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

    struct DecompressionStage
    {
        bool allow_different_codecs = false;
    };

    std::optional<SourceStage> source;
    bool gather = false;
    std::optional<DiskCacheStage> disk_cache;
    std::optional<MemoryCacheStage> memory_cache;
    bool distributed_cache = false;
    std::optional<AsyncPrefetchStage> async_prefetch;
    std::vector<DecryptionStage> decryption_stages;
    std::optional<DecompressionStage> decompression;
    std::optional<size_t> buffer_size_override;
    std::optional<ReadSettings> gather_settings_override;
};

}
