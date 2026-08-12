#include <IO/preadNoWait.h>

#include <base/MemorySanitizer.h>
#include <base/errnoToString.h>
#include <Common/VersionNumber.h>
#include <Poco/Environment.h>

#include <fmt/format.h>

#include <cerrno>

#if defined(OS_LINUX)

#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

/// We don't want to depend on specific glibc version.

#if !defined(RWF_NOWAIT)
    #define RWF_NOWAIT 8
#endif

#if !defined(SYS_preadv2)
    #if defined(__x86_64__)
        #define SYS_preadv2 327
    #elif defined(__aarch64__)
        #define SYS_preadv2 286
    #elif defined(__powerpc64__)
        #define SYS_preadv2 380
    #elif defined(__riscv)
        #define SYS_preadv2 286
    #elif defined(__loongarch64)
        #define SYS_preadv2 286
    #elif defined(__e2k__)
        #define SYS_preadv2 395
    #else
        #error "Unsupported architecture"
    #endif
#endif

#endif


namespace DB
{

ssize_t preadNoWait(
    [[maybe_unused]] int fd, [[maybe_unused]] char * buf, [[maybe_unused]] size_t size, [[maybe_unused]] size_t offset)
{
#if defined(OS_LINUX)
    struct iovec io_vec{ .iov_base = buf, .iov_len = size };

    ssize_t res = syscall(
        SYS_preadv2, fd,
        &io_vec, 1,
        /// This is kind of weird calling convention for syscall.
        static_cast<int64_t>(offset), static_cast<int64_t>(offset >> 32),
        /// This flag forces read from page cache or returning EAGAIN.
        RWF_NOWAIT);

    if (res > 0)
        __msan_unpoison(buf, res);

    return res;
#else
    errno = ENOSYS;
    return -1;
#endif
}

bool isPreadNoWaitUnavailable(int error)
{
    /// ENOSYS - the kernel does not implement the system call.
    /// EOPNOTSUPP - the kernel or the filesystem does not support the flag.
    /// EPERM - the system call is not in the allow list of a `seccomp` profile;
    /// this is how container runtimes reject the system calls they don't know about.
    return error == ENOSYS || error == EOPNOTSUPP || error == EPERM;
}

bool isPreadNoWaitProbeRejected(ssize_t res, int error)
{
    /// The probe passes an invalid file descriptor, and the kernel looks the descriptor up
    /// only after the `seccomp` filters and the system call table let the call through,
    /// so `EBADF` is the only answer that proves the system call actually ran.
    /// Anything else - a `seccomp` filter substituting an arbitrary `errno` (`SECCOMP_RET_ERRNO`)
    /// or even a success - means the call was intercepted and cannot be used.
    return res != -1 || error != EBADF;
}

namespace
{

String probePreadNoWait()
{
#if !defined(OS_LINUX)
    return "`preadv2` is only available on Linux";
#else
    /// According to man, Linux 5.9 and 5.10 have a bug in preadv2() with the RWF_NOWAIT:
    /// it can return 0 while not at the end of the file, which is indistinguishable from a real EOF.
    /// https://manpages.debian.org/testing/manpages-dev/preadv2.2.en.html#BUGS
    /// We also don't use it on older Linux kernels, because according to user's reports,
    /// RedHat-patched kernels might be also affected.
    VersionNumber linux_version(Poco::Environment::osVersion());
    if (linux_version < VersionNumber{5, 11, 0})
        return fmt::format(
            "the Linux kernel is {}, and `preadv2` with the `RWF_NOWAIT` flag can report the end of the file instead of "
            "the data before Linux 5.11 (see the BUGS section of `man 2 preadv2`); upgrade the kernel to 5.11 or newer",
            linux_version.toString());

    /// The system call can also be unavailable regardless of the kernel version:
    /// a `seccomp` profile of a container runtime can reject it.
    /// An invalid file descriptor is passed on purpose - `seccomp` filters and the system call table
    /// are consulted before the descriptor is looked up, so a system call that is allowed
    /// answers `EBADF` without reading anything, and any other answer means it was intercepted.
    /// The per-read classification in `isPreadNoWaitUnavailable` stays narrower: a read from a real
    /// descriptor fails with `EAGAIN`, `EINTR` or `EIO` even when the system call itself works.
    char buf[1] = {};
    ssize_t res = preadNoWait(-1, buf, sizeof(buf), 0);
    if (isPreadNoWaitProbeRejected(res, errno))
        return fmt::format(
            "the `preadv2` system call is not available (the probe with an invalid file descriptor answered {} "
            "instead of `EBADF`); it is typically rejected by a `seccomp` profile of a container runtime, "
            "and can be allowed in the runtime configuration",
            res == -1 ? errnoToString(errno) : std::to_string(res));

    return {};
#endif
}

}

const String & preadNoWaitUnavailableReason()
{
    static const String reason = probePreadNoWait();
    return reason;
}

}
