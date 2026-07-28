
#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Common/EventFD.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <base/defines.h>
#include <unistd.h>

#if defined(OS_LINUX)
#include <sys/eventfd.h>
#else
#include <fcntl.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PIPE;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_WRITE_TO_SOCKET;
}

EventFD::EventFD()
{
#if defined(OS_LINUX)
    /// CLOEXEC so the wakeup fd is not inherited by forked/exec'd children.
    fd = eventfd(0 /* initval */, EFD_CLOEXEC /* flags */);
    if (fd == -1)
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot create eventfd");
#else
    /// macOS has no eventfd; emulate it with a self-pipe (see EventFD.h). `fd` is the blocking
    /// read end (so read() waits for a write()), `write_fd` is the non-blocking write end (so
    /// write() can report a full pipe via EAGAIN).
    int pipe_fd[2];
    if (-1 == pipe(pipe_fd))
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot create pipe for eventfd emulation");

    /// CLOEXEC on both ends so the wakeup fds are not inherited by forked/exec'd children,
    /// and O_NONBLOCK on the write end so write() reports a full pipe via EAGAIN.
    bool ok = -1 != fcntl(pipe_fd[0], F_SETFD, FD_CLOEXEC) && -1 != fcntl(pipe_fd[1], F_SETFD, FD_CLOEXEC);
    if (ok)
    {
        int flags = fcntl(pipe_fd[1], F_GETFL, 0);
        ok = -1 != flags && -1 != fcntl(pipe_fd[1], F_SETFL, flags | O_NONBLOCK);
    }
    if (!ok)
    {
        [[maybe_unused]] int e0 = close(pipe_fd[0]);
        [[maybe_unused]] int e1 = close(pipe_fd[1]);
        throw ErrnoException(ErrorCodes::CANNOT_PIPE, "Cannot configure eventfd emulation pipe");
    }

    fd = pipe_fd[0];
    write_fd = pipe_fd[1];
#endif
}

uint64_t EventFD::read() const
{
    uint64_t buf = 0;
    while (-1 == ::read(fd, &buf, sizeof(buf)))
    {
        if (errno == EAGAIN)
            break;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot read from eventfd");
    }

    return buf;
}

bool EventFD::write(uint64_t increase) const
{
#if defined(OS_LINUX)
    const int write_to = fd;
#else
    const int write_to = write_fd;
#endif

    while (-1 == ::write(write_to, &increase, sizeof(increase)))
    {
        if (errno == EAGAIN)
            return false;

        if (errno != EINTR)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_SOCKET, "Cannot write to eventfd");
    }

    return true;
}

EventFD::~EventFD()
{
    if (fd != -1)
    {
        [[maybe_unused]] int err = close(fd);
        chassert(!err || errno == EINTR);
    }
#if defined(OS_DARWIN)
    if (write_fd != -1)
    {
        [[maybe_unused]] int err = close(write_fd);
        chassert(!err || errno == EINTR);
    }
#endif
}

}

#endif
