#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

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

/// Whether a local read of `estimated_size` bytes will be performed with O_DIRECT.
///
/// Mirrors `createReadBufferFromFileBase`, including its platform guard: O_DIRECT is only ever
/// attempted under Linux and FreeBSD, so on every other platform the answer is 'no' regardless
/// of the threshold. A component that resolves the read method before the buffer is created
/// (e.g. `DiskLocal::prepareRead`) must use this, so that it does not claim an O_DIRECT read
/// where the reader cannot perform one.
bool willUseDirectIO(size_t estimated_size, size_t direct_io_threshold);

/// Whether the file can actually be opened with O_DIRECT.
///
/// `willUseDirectIO` proves only that `createReadBufferFromFileBase` will *attempt* O_DIRECT.
/// The open can still be rejected at runtime (e.g. the filesystem does not support O_DIRECT),
/// and then the reader falls back to cached IO and resolves the method as a non-direct read.
/// A component that branches on `direct_io` before the buffer is created can use this probe -
/// it opens the file with the same flags the reader uses - to prove the attempt will succeed
/// instead of assuming it. On platforms where the reader never attempts O_DIRECT the answer
/// is 'no', matching `willUseDirectIO`.
bool canOpenWithDirectIO(const std::string & path);

/// The read method to use for a local file, given the requested one.
///
/// When `preadNoWait` is unavailable, 'pread_threadpool' falls back to regular 'pread'.
/// Reads with O_DIRECT are not affected.
///
/// Each call site passes its own view of `direct_io`, derived from its own estimate of the
/// read size. A component that decides based on the read method before the buffer is created
/// (e.g. `DiskLocal::prepareRead`) may estimate the size differently from the component that
/// creates the buffer, and then the two resolve different methods for the same read.
/// So the fallback is per decision point, not a global property of the read.
LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool pread_no_wait_supported, bool direct_io);

/// The same resolution with the support probed on demand (see `getPreadNoWaitSupport`).
/// The probe is a raw `preadv2` system call, and a `seccomp` profile that kills the process
/// on unknown system calls must only see it when the check is actually needed,
/// so neither another read method nor an O_DIRECT read reaches the probe.
LocalFSReadMethod resolveLocalFSReadMethod(LocalFSReadMethod requested, bool direct_io);

}
