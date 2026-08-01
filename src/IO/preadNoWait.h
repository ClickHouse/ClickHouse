#pragma once

#include <base/types.h>

#include <sys/types.h>


namespace DB
{

/// Reads data at the specified offset, but only if it is already in the page cache,
/// and fails with `EAGAIN` otherwise, without waiting for the disk.
/// This is `preadv2` with the `RWF_NOWAIT` flag, which exists only on Linux;
/// on other systems it fails with `ENOSYS`.
/// Returns the number of bytes read, 0 at the end of the file, or -1 with `errno` set.
ssize_t preadNoWait(int fd, char * buf, size_t size, size_t offset);

/// Whether this `errno` from `preadNoWait` means that the system call cannot be used at all,
/// as opposed to a failure to read the data.
bool isPreadNoWaitUnavailable(int error);

/// Whether `preadNoWait` can be used on this system.
/// The `pread_threadpool` read method needs it to read the data that is already in the page cache
/// in the calling thread; without it, every read is handed off to a thread pool, which is expensive.
struct PreadNoWaitSupport
{
    bool supported = false;
    /// Empty if supported. Otherwise, explains what is wrong with this system,
    /// to be reported in `system.warnings`.
    String unsupported_reason;
};

/// The system is probed once, on the first call.
const PreadNoWaitSupport & getPreadNoWaitSupport();

}
