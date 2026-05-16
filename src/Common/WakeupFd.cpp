#include <Common/WakeupFd.h>

#include <Common/Exception.h>
#include <Common/ErrnoException.h>

#include <unistd.h>

#include <cerrno>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
}

WakeupFd::WakeupFd()
{
    /// PipeFDs constructor already opens the pipe with CLOEXEC; flip both ends to non-blocking.
    pipe.setNonBlockingReadWrite();
}

void WakeupFd::notify() const
{
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
    char buf[64];
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
