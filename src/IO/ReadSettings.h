#pragma once

#include <cstddef>
#include <Core/Defines.h>
#include <IO/DistributedCacheSettings.h>
#include <IO/ReadMethod.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/UserInfo.h>
#include <Common/Priority.h>
#include <Common/Scheduler/ResourceLink.h>
#include <Common/IThrottler.h>

namespace DB
{

class MMappedFileCache;
class PageCache;
class Context;

struct ReadSettings
{
    ReadSettings() = default;
    explicit ReadSettings(const Context & context);

    /// Method to use reading from local filesystem.
    LocalFSReadMethod local_fs_method = LocalFSReadMethod::pread;
    /// Method to use reading from remote filesystem.
    RemoteFSReadMethod remote_fs_method = RemoteFSReadMethod::threadpool;

    /// https://eklitzke.org/efficient-file-copying-on-linux
    size_t local_fs_buffer_size = 128 * 1024;

    size_t remote_fs_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t prefetch_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    bool local_fs_prefetch = false;
    bool remote_fs_prefetch = false;

    /// For 'read', 'pread' and 'pread_threadpool' methods.
    size_t direct_io_threshold = 0;

    /// For 'mmap' method.
    size_t mmap_threshold = 0;
    MMappedFileCache * mmap_cache = nullptr;

    /// For 'pread_threadpool'/'io_uring' method. Lower value is higher priority.
    Priority priority;

    bool load_marks_asynchronously = true;

    size_t remote_fs_read_max_backoff_ms = 10000;
    size_t remote_fs_read_backoff_max_tries = 4;

    bool enable_filesystem_read_prefetches_log = false;

    bool enable_filesystem_cache = true;
    bool read_from_filesystem_cache_if_exists_otherwise_bypass_cache = false;
    bool enable_filesystem_cache_log = false;
    size_t filesystem_cache_segments_batch_size = 20;
    size_t filesystem_cache_reserve_space_wait_lock_timeout_milliseconds = 1000;
    bool filesystem_cache_allow_background_download = true;
    bool filesystem_cache_allow_background_download_for_metadata_files_in_packed_storage = true;
    bool filesystem_cache_allow_background_download_during_fetch = true;
    bool filesystem_cache_prefer_bigger_buffer_size = true;
    std::optional<size_t> filesystem_cache_boundary_alignment;

    bool use_page_cache_for_disks_without_file_cache = false;
    [[ maybe_unused ]] bool use_page_cache_with_distributed_cache = false;
    bool read_from_page_cache_if_exists_otherwise_bypass_cache = false;
    bool page_cache_inject_eviction = false;
    size_t page_cache_block_size = 1 << 20;
    size_t page_cache_lookahead_blocks = 16;
    std::shared_ptr<PageCache> page_cache;

    size_t filesystem_cache_max_download_size = (128UL * 1024 * 1024 * 1024);
    bool filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit = true;

    size_t remote_read_min_bytes_for_seek = DBMS_DEFAULT_BUFFER_SIZE;

    bool remote_read_buffer_restrict_seek = false;
    bool remote_read_buffer_use_external_buffer = false;

    /// Bandwidth throttler to use during reading
    ThrottlerPtr remote_throttler;
    ThrottlerPtr local_throttler;

    IOSchedulingSettings io_scheduling;

    size_t http_max_tries = 10;
    size_t http_retry_initial_backoff_ms = 100;
    size_t http_retry_max_backoff_ms = 1600;
    bool http_skip_not_found_url_for_globs = true;
    bool http_make_head_request = true;

    bool read_through_distributed_cache = false;
    DistributedCacheSettings distributed_cache_settings;
    std::optional<FileCacheUserInfo> filecache_user_info;
    bool enable_hdfs_pread = true;

    ReadSettings adjustBufferSize(size_t file_size) const
    {
        ReadSettings res = *this;
        res.local_fs_buffer_size = std::min(std::max(1ul, file_size), local_fs_buffer_size);
        res.remote_fs_buffer_size = std::min(std::max(1ul, file_size), remote_fs_buffer_size);
        res.prefetch_buffer_size = std::min(std::max(1ul, file_size), prefetch_buffer_size);
        return res;
    }

    ReadSettings withNestedBuffer() const
    {
        ReadSettings res = *this;
        res.remote_read_buffer_restrict_seek = true;
        res.remote_read_buffer_use_external_buffer = true;
        return res;
    }
};

ReadSettings getReadSettings();

ReadSettings getReadSettingsForMetadata();
}
