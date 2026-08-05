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

/// The read method to use for a local file, given the requested one.
///
/// 'pread_threadpool' pays for a thread pool hand-off on every read, and it pays off only because
/// the data that is already in the page cache is read in the calling thread instead - see
/// `preadNoWait`. When that is not possible, 'pread' is used instead: it reads the same data
/// in the calling thread and never hands anything off.
///
/// Reads with O_DIRECT never look at the page cache, and are always performed in the thread pool,
/// so they are not affected.
LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool pread_no_wait_supported, bool direct_io);

/// The same resolution with the support probed on demand (see `getPreadNoWaitSupport`).
/// The probe is a raw `preadv2` system call, and a `seccomp` profile that kills the process
/// on unknown system calls must only see it when 'pread_threadpool' is actually requested,
/// so no other read method reaches the probe.
LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool direct_io);

}
