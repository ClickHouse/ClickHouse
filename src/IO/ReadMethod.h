#pragma once

#include <cstdint>

namespace DB
{

enum class LocalFSReadMethod : uint8_t
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
     * Use the io_uring Linux subsystem for asynchronous reads.
     * Can use direct IO after specified size.
     * Can do prefetch with double buffering.
     */
    io_uring,

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

enum class RemoteFSReadMethod : uint8_t
{
    read,
    threadpool,
};

}
