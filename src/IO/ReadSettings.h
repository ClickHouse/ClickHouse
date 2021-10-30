#pragma once

#include <cstddef>
#include <string>
#include <Core/Defines.h>

namespace DB
{
enum class LocalFSReadMethod
{
    /**
     * Simple synchronous reads with 'read'.
     * Can use direct IO after specified size.
     * Can use prefetch by asking OS to perform readahead.
     */
    read,

    /**
     * Simple synchronous reads with 'pread'.
     * In contrast to 'read', shares single file descriptor from multiple threads.
     * Can use direct IO after specified size.
     * Can use prefetch by asking OS to perform readahead.
     */
    pread,

    /**
     * Use mmap after specified size or simple synchronous reads with 'pread'.
     * Can use prefetch by asking OS to perform readahead.
     */
    mmap,

    /**
     * Checks if data is in page cache with 'preadv2' on modern Linux kernels.
     * If data is in page cache, read from the same thread.
     * If not, offload IO to separate threadpool.
     * Can do prefetch with double buffering.
     * Can use specified priorities and limit the number of concurrent reads.
     */
    pread_threadpool,

    /// Use asynchronous reader with fake backend that in fact synchronous.
    /// @attention Use only for testing purposes.
    pread_fake_async
};

enum class RemoteFSReadMethod
{
    read,
    read_threadpool,
};

class MMappedFileCache;

struct ReadSettings
{
    /// Method to use reading from local filesystem.
    LocalFSReadMethod local_fs_method = LocalFSReadMethod::pread;
    /// Method to use reading from remote filesystem.
    RemoteFSReadMethod remote_fs_method = RemoteFSReadMethod::read;

    size_t local_fs_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;
    size_t remote_fs_buffer_size = DBMS_DEFAULT_BUFFER_SIZE;

    bool local_fs_prefetch = false;
    bool remote_fs_prefetch = false;

    /// For 'read', 'pread' and 'pread_threadpool' methods.
    size_t direct_io_threshold = 0;

    /// For 'mmap' method.
    size_t mmap_threshold = 0;
    MMappedFileCache * mmap_cache = nullptr;

    /// For 'pread_threadpool' method. Lower is more priority.
    size_t priority = 0;

    size_t remote_fs_read_max_backoff_ms = 10000;
    size_t remote_fs_read_backoff_max_tries = 4;

    /// Set to true for MergeTree tables to make sure
    /// that last position (offset in compressed file) is always passed.
    /// (Otherwise asynchronous reading from remote fs is not efficient).
    /// If reading is done without final position set, throw logical_error.
    bool must_read_until_position = false;

    ReadSettings adjustBufferSize(size_t file_size) const
    {
        ReadSettings res = *this;
        res.local_fs_buffer_size = std::min(file_size, local_fs_buffer_size);
        res.remote_fs_buffer_size = std::min(file_size, remote_fs_buffer_size);
        return res;
    }
};

}
