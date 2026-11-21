#pragma once

/// With the bundled musl, the file descriptors visible to the program are not the kernel ones:
/// every syscall made through libc translates them, and the program sees a pseudorandom encoding
/// of the kernel descriptor and its generation, which changes when the descriptor is closed.
/// A descriptor used after it was closed gets `EBADF` instead of reaching an unrelated file that
/// reused the number. See `contrib/musl/src/internal/fdmap.h`.
///
/// The kernel numbers are needed where a descriptor bypasses the syscalls of libc:
/// io_uring submission entries, descriptors of a child process after `exec`, and so on.

#if defined(USE_MUSL)
#    include <unistd.h> /// __musl_fd_to_kernel, __musl_fd_from_kernel
#endif

namespace DB
{

/// Returns the kernel descriptor, or -1 if `fd` is not open.
/// Negative values and 0, 1, 2 are returned unchanged.
inline int toKernelFileDescriptor(int fd)
{
#if defined(USE_MUSL)
    return __musl_fd_to_kernel(fd);
#else
    return fd;
#endif
}

/// Returns the descriptor of the program that refers to the given kernel descriptor. It is used to
/// place a descriptor at a specific kernel number with `dup2`, e.g. for a child process.
inline int fromKernelFileDescriptor(int kernel_fd)
{
#if defined(USE_MUSL)
    return __musl_fd_from_kernel(kernel_fd);
#else
    return kernel_fd;
#endif
}

}
