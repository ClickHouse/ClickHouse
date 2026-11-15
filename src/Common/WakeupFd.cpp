#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/WakeupFd.h>

#include <cerrno>
#include <unistd.h>

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <fcntl.h>
#include <sys/stat.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
#ifdef DEBUG_OR_SANITIZER_BUILD
    extern const int CANNOT_FSTAT;
    extern const int LOGICAL_ERROR;
#endif
}

#ifdef DEBUG_OR_SANITIZER_BUILD
namespace
{

FdIdentity getIdentity(int fd)
{
    struct stat st{};
    if (0 != fstat(fd, &st))
        throw ErrnoException(ErrorCodes::CANNOT_FSTAT, "Cannot fstat wakeup pipe");
    return {static_cast<UInt64>(st.st_dev), static_cast<UInt64>(st.st_ino)};
}

}
#endif

WakeupFd::WakeupFd()
{
    /// PipeFDs constructor already opens the pipe with CLOEXEC; flip both ends to non-blocking.
    pipe.setNonBlockingReadWrite();

#ifdef DEBUG_OR_SANITIZER_BUILD
    read_end_identity = getIdentity(pipe.fds_rw[0]);
    write_end_identity = getIdentity(pipe.fds_rw[1]);
#endif
}


void WakeupFd::validate(PipeEnd end [[maybe_unused]]) const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    const bool is_read = end == PipeEnd::Read;
    const char * side = is_read ? "read" : "write";
    int fd = is_read ? pipe.fds_rw[0] : pipe.fds_rw[1];
    const FdIdentity & expected = is_read ? read_end_identity : write_end_identity;

    int flags = fcntl(fd, F_GETFL);
    if (flags == -1)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Wakeup pipe {} end (fd {}) is invalid ({}); the fd was probably closed by unrelated code",
            side,
            fd,
            errnoToString());

    if (!(flags & O_NONBLOCK))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Wakeup pipe {} end (fd {}) lost O_NONBLOCK (flags {:#x}); the fd was probably tampered with by unrelated code",
            side,
            fd,
            flags);

    struct stat st{};
    if (0 != fstat(fd, &st))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot fstat wakeup pipe {} end (fd {}): {}", side, fd, errnoToString());

    if (!S_ISFIFO(st.st_mode) || static_cast<UInt64>(st.st_dev) != expected.dev || static_cast<UInt64>(st.st_ino) != expected.ino)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Wakeup pipe {} end (fd {}) refers to another file (mode {:#o}, dev:ino {}:{}, expected {}:{}); "
            "the fd was probably closed and recycled by unrelated code",
            side,
            fd,
            st.st_mode,
            static_cast<UInt64>(st.st_dev),
            static_cast<UInt64>(st.st_ino),
            expected.dev,
            expected.ino);
#endif
}

void WakeupFd::notify() const
{
    validate(PipeEnd::Write);

    const char byte = '\0';
    while (true)
    {
        ssize_t r = ::write(pipe.fds_rw[1], &byte, 1);
        if (r == 1)
            return;
        if (r < 0 && errno == EINTR)
            continue;
        if (r < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
            return;   /// pipe full -> wakeup already pending
        throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write to wakeup pipe");
    }
}

void WakeupFd::drain() const
{
    validate(PipeEnd::Read);

    char buf[PIPE_BUF];
    while (true)
    {
        ssize_t r = ::read(pipe.fds_rw[0], buf, sizeof(buf));
        if (r > 0)
            continue;
        if (r == 0)
            return;   /// write end closed; nothing more to read
        if (errno == EINTR)
            continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK)
            return;
        throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from wakeup pipe");
    }
}

}
