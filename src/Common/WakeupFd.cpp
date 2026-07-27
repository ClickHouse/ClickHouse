#include <Common/WakeupFd.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <unistd.h>

#include <cerrno>

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

WakeupFd::WakeupFd()
{
    /// PipeFDs constructor already opens the pipe with CLOEXEC; flip both ends to non-blocking.
    pipe.setNonBlockingReadWrite();

#ifdef DEBUG_OR_SANITIZER_BUILD
    for (int which : {0, 1})
    {
        struct stat st{};
        if (0 != fstat(pipe.fds_rw[which], &st))
            throw ErrnoException(ErrorCodes::CANNOT_FSTAT, "Cannot fstat wakeup pipe");
        ends[which] = {static_cast<UInt64>(st.st_dev), static_cast<UInt64>(st.st_ino)};
    }
#endif
}

#ifdef DEBUG_OR_SANITIZER_BUILD
std::optional<PreformattedMessage> WakeupFd::checkEnd(int which) const
{
    const char * side = which == 0 ? "read" : "write";
    int fd = pipe.fds_rw[which];

    int flags = fcntl(fd, F_GETFL);
    if (flags == -1)
        return PreformattedMessage::create(
            "Wakeup pipe {} end (fd {}) is invalid ({}); the fd was probably closed by unrelated code",
            side,
            fd,
            errnoToString());
    if (!(flags & O_NONBLOCK))
        return PreformattedMessage::create(
            "Wakeup pipe {} end (fd {}) lost O_NONBLOCK (flags {:#x}); the fd was probably tampered with by unrelated code",
            side,
            fd,
            flags);

    struct stat st{};
    if (0 != fstat(fd, &st))
        return PreformattedMessage::create("Cannot fstat wakeup pipe {} end (fd {}): {}", side, fd, errnoToString());
    if (!S_ISFIFO(st.st_mode) || static_cast<UInt64>(st.st_dev) != ends[which].dev || static_cast<UInt64>(st.st_ino) != ends[which].ino)
        return PreformattedMessage::create(
            "Wakeup pipe {} end (fd {}) refers to another file (mode {:#o}, dev:ino {}:{}, expected {}:{}); "
            "the fd was probably closed and recycled by unrelated code",
            side,
            fd,
            st.st_mode,
            static_cast<UInt64>(st.st_dev),
            static_cast<UInt64>(st.st_ino),
            ends[which].dev,
            ends[which].ino);

    return std::nullopt;
}

void WakeupFd::validate(int which) const
{
    if (auto problem = checkEnd(which))
        throw Exception(std::move(*problem), ErrorCodes::LOGICAL_ERROR);
}
#endif

void WakeupFd::notify() const
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    validate(1);
#endif

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
#ifdef DEBUG_OR_SANITIZER_BUILD
    validate(0);
#endif

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
