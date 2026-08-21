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
/// as opposed to a failure to read this data at this moment.
bool isPreadNoWaitUnavailable(int error);

/// Classifies the result of the support probe below, which passes an invalid file descriptor:
/// failing with `EBADF` is the only answer that proves the system call actually ran, so any other
/// result - e.g. a `seccomp` filter substituting an arbitrary `errno` - means it cannot be used.
bool isPreadNoWaitProbeRejected(ssize_t res, int error);

/// Whether `preadNoWait` can be used on this system: the kernel has to be new enough, and the
/// system call must not be rejected by a `seccomp` profile of a container runtime.
/// Returns an empty string if it can be used, and the reason why it cannot otherwise.
/// The system is probed once, on the first call.
const String & preadNoWaitUnavailableReason();

}
