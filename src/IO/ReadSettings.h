#pragma once

#include <cstddef>
#include <Core/Defines.h>
#include <IO/DistributedCacheSettings.h>
#include <IO/ReadMethod.h>
#include <Interpreters/FileCache/FileCache_fwd.h>
#include <Interpreters/FileCache/FileCacheOriginInfo.h>
#include <Common/Priority.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/IThrottler.h>

namespace DB
{

class MMappedFileCache;
class PageCache;
class Context;

/// Settings controlling reads from a remote filesystem (S3, Azure, HDFS, GCS, …).
/// Used by the transport-level read buffers (`ReadBufferFromS3`, `ReadBufferFromHDFS`, …).
struct RemoteFSReadSettings
{
    /// Method to use reading from remote filesystem (read | threadpool).
    RemoteFSReadMethod method = RemoteFSReadMethod::threadpool;

    /// Buffer size for remote filesystem reads.
    size_t buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    /// Enable async prefetching for remote reads.
    bool prefetch = false;

    /// Retry policy for transient remote read errors.
    size_t max_backoff_ms = 10000;
    size_t max_retries = 4;

    /// Minimum bytes between successive reads before a seek becomes a new request.
    size_t min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE;

    /// Floor for `buffer_size` when filesystem cache is active, to reduce cache fragmentation.
    /// Backed by the public Setting `prefetch_buffer_size` (name preserved for compatibility).
    size_t large_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    /// HDFS-specific: use pread for non-async reads.
    bool enable_hdfs_pread = true;

    /// Log every blob storage read operation to system.blob_storage_log.
    bool enable_blob_storage_log = false;
};

/// Settings controlling reads from the local filesystem.
/// Used by `createReadBufferFromFileBase` to pick a read method and buffer size.
struct LocalFSReadSettings
{
    /// Method to use reading from local filesystem.
    LocalFSReadMethod method = LocalFSReadMethod::pread;

    /// https://eklitzke.org/efficient-file-copying-on-linux
    size_t buffer_size = 128 * 1024;

    bool prefetch = false;

    /// For 'read', 'pread' and 'pread_threadpool' methods.
    size_t direct_io_threshold = 0;

    /// For 'mmap' method.
    size_t mmap_threshold = 0;
    MMappedFileCache * mmap_cache = nullptr;
};

/// Settings controlling HTTP transport retry/backoff and behavior.
/// Used by `ReadWriteBufferFromHTTP`.
struct HTTPReadSettings
{
    size_t max_tries = 10;
    size_t retry_initial_backoff_ms = 100;
    size_t retry_max_backoff_ms = 1600;
    bool skip_not_found_url_for_globs = true;
    bool make_head_request = true;
};

/// Settings controlling the in-memory page cache behavior.
/// Used by CachedInMemoryReadBufferFromFile and the ReadPipeline memory cache stage.
struct PageCacheSettings
{
    bool read_if_exists_otherwise_bypass = false;
    /// Test-only: randomly evict cache entries to exercise the eviction path.
    bool random_eviction_for_tests = false;
    size_t block_size = 1 << 20;
    size_t lookahead_blocks = 16;
    size_t max_coalesced_bytes = 16 << 20;
    /// The page-cache instance (shared global cache). Null when the stage is disabled.
    std::shared_ptr<PageCache> cache;
};

/// Settings controlling the filesystem (disk) cache behavior.
/// Used by CachedOnDiskReadBufferFromFile and the ReadPipeline disk cache stage.
struct FilesystemCacheSettings
{
    bool read_if_exists_otherwise_bypass = false;
    size_t segments_batch_size = 20;
    std::optional<size_t> boundary_alignment;
    bool allow_background_download = true;
    bool allow_background_download_for_metadata_files_in_packed_storage = true;
    bool allow_background_download_during_fetch = true;
    /// Hint to callers (DiskObjectStorage / StorageObjectStorageSource) to enlarge the remote-FS
    /// read buffer when this cache is active — reduces cache fragmentation. Not the cache's own buffer.
    bool prefer_large_caller_buffer = true;
    size_t reserve_space_wait_lock_timeout_milliseconds = 1000;
    size_t max_download_size_per_query = (128UL * 1024 * 1024 * 1024);
    bool skip_download_if_exceeds_per_query_cache_write_limit = true;
    bool enable_log = false;
    /// Request-origin metadata (user_id, client_weight) passed to FileCache::getOrSet.
    std::optional<FileCacheOriginInfo> request_origin_info;
};

struct ReadSettings
{
    /// Local filesystem source parameters (read method, buffer size, mmap/direct-io thresholds).
    LocalFSReadSettings local_fs_settings;

    /// Remote filesystem source parameters (S3/Azure/HDFS/GCS read method, buffer size, retry policy).
    RemoteFSReadSettings remote_fs_settings;

    /// For 'pread_threadpool'/'io_uring' method and async prefetch. Lower value is higher priority.
    Priority priority;

    bool enable_filesystem_read_prefetches_log = false;

    /// Toggle for the filesystem cache stage. The stage parameters live in `filesystem_cache_settings`.
    bool enable_filesystem_cache = true;
    FilesystemCacheSettings filesystem_cache_settings;

    /// Toggles for the page cache stage (decided per-disk before composing the pipeline).
    /// The stage parameters live in `page_cache_settings`.
    bool use_page_cache_for_disks_without_file_cache = false;
    bool use_page_cache_with_distributed_cache = false;
    bool use_page_cache_for_local_disks = false;
    bool use_page_cache_for_object_storage = false;
    PageCacheSettings page_cache_settings;

    /// Bandwidth throttler to use during reading
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    IOSchedulingSettings io_scheduling;

    /// HTTP transport retry/backoff parameters.
    HTTPReadSettings http_settings;

    bool read_through_distributed_cache = false;
    DistributedCacheSettings distributed_cache_settings;

    ReadSettings adjustBufferSize(size_t file_size) const;

    /// Verification/metadata-read mode: disable every read-side cache and the
    /// per-operation logs. Used by checkDataPart, restore, and iceberg metadata reads.
    void disableCachesAndLogging();

    /// Configure the remote-FS source for reading a small object-storage file
    /// (sets threadpool method, disables prefetch, shrinks the buffer). Used by
    /// metadata-storage operations that fetch small per-object files.
    void useForSmallRemoteRead(size_t buffer_size);
};

ReadSettings getReadSettings();

ReadSettings getReadSettingsForMetadata();
}
