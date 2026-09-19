#include <Common/Socket.h>

#include <Common/ErrnoException.h>
#include <Common/NetException.h>

#include <algorithm>
#include <chrono>
#include <limits>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#include <winsock2.h>
#else
#include <cerrno>
#include <poll.h>
#include <sys/socket.h>
#endif


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_POLL;
}

namespace
{

/// `poll`/`WSAPoll` take a millisecond timeout. Round a non-zero wait up rather than down: a
/// sub-millisecond timeout must not collapse into a busy zero-timeout poll, and waiting a
/// fraction of a millisecond too long is harmless.
int toPollTimeoutMilliseconds(Int64 timeout_microseconds)
{
    if (timeout_microseconds < 0)
        return -1;
    if (timeout_microseconds == 0)
        return 0;
    return static_cast<int>(std::max<Int64>(1, (timeout_microseconds + 999) / 1000));
}

}

#if defined(OS_WINDOWS)

/// `SOCKET` is what Winsock takes; `Handle` is the same value widened to an integer type that
/// does not drag <winsock2.h> into the header.
static SOCKET toWinsock(Socket::Handle handle)
{
    return static_cast<SOCKET>(handle);
}

int Socket::poll(int events, Int64 timeout_microseconds) const
{
    if (!isValid())
        throw NetException(ErrorCodes::CANNOT_POLL, "Cannot wait on an invalid socket");

    WSAPOLLFD fd{};
    fd.fd = toWinsock(handle);
    /// `POLLRDNORM`/`POLLWRNORM` rather than `POLLIN`/`POLLOUT`: `POLLIN` also covers
    /// out-of-band data, which we never wait for.
    if (events & Read)
        fd.events |= POLLRDNORM;
    if (events & Write)
        fd.events |= POLLWRNORM;

    auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(std::max<Int64>(0, timeout_microseconds));

    while (true)
    {
        Int64 remaining = timeout_microseconds;
        if (timeout_microseconds > 0)
        {
            remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - std::chrono::steady_clock::now()).count();
            if (remaining <= 0)
                return None;
        }

        const int ready = WSAPoll(&fd, 1, toPollTimeoutMilliseconds(remaining));

        if (ready == 0)
            return None;

        if (ready == SOCKET_ERROR)
        {
            const int error = lastError();
            if (isInterrupted(error))
                continue;
            throw NetException(ErrorCodes::CANNOT_POLL, "Cannot wait on socket (WSAPoll), error code: {}", error);
        }

        int result = None;
        if (fd.revents & POLLRDNORM)
            result |= Read;
        if (fd.revents & POLLWRNORM)
            result |= Write;
        /// Reported whether or not it was asked for, as on POSIX.
        if (fd.revents & (POLLERR | POLLHUP | POLLNVAL))
            result |= Error;
        return result;
    }
}

Int64 Socket::peek(char * buffer, size_t size) const
{
    /// Winsock has no per-call `MSG_DONTWAIT`: whether a call blocks is a property of the
    /// socket, and this must not change that for a socket it does not own. So ask first, with a
    /// zero timeout, whether anything is readable - if nothing is, that is the would-block case
    /// the POSIX branch reports through `EAGAIN` - and only then peek, which can no longer block.
    if (!(poll(Read, 0) & Read))
    {
        ::WSASetLastError(WSAEWOULDBLOCK);
        return -1;
    }

    /// `recv` takes an `int` length on Windows.
    const int to_peek = static_cast<int>(std::min<size_t>(size, std::numeric_limits<int>::max()));
    const int res = ::recv(toWinsock(handle), buffer, to_peek, MSG_PEEK);
    return res == SOCKET_ERROR ? -1 : res;
}

int Socket::lastError()
{
    return ::WSAGetLastError();
}

bool Socket::isWouldBlock(int error)
{
    return error == WSAEWOULDBLOCK;
}

bool Socket::isInterrupted(int error)
{
    /// Winsock only reports this to a thread whose blocking call was cancelled by the long-gone
    /// `WSACancelBlockingCall`, so in practice it never happens - but the retry loops read
    /// better for being symmetric with POSIX.
    return error == WSAEINTR;
}

#else

int Socket::poll(int events, Int64 timeout_microseconds) const
{
    if (!isValid())
        throw NetException(ErrorCodes::CANNOT_POLL, "Cannot wait on an invalid socket");

    pollfd fd{};
    fd.fd = handle;
    if (events & Read)
        fd.events |= POLLIN;
    if (events & Write)
        fd.events |= POLLOUT;

    auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(std::max<Int64>(0, timeout_microseconds));

    while (true)
    {
        Int64 remaining = timeout_microseconds;
        if (timeout_microseconds > 0)
        {
            /// Recompute rather than reusing the original timeout, so that a stream of signals
            /// cannot extend the wait indefinitely.
            remaining = std::chrono::duration_cast<std::chrono::microseconds>(deadline - std::chrono::steady_clock::now()).count();
            if (remaining <= 0)
                return None;
        }

        const int ready = ::poll(&fd, 1, toPollTimeoutMilliseconds(remaining));

        if (ready == 0)
            return None;

        if (ready < 0)
        {
            const int error = lastError();
            if (isInterrupted(error))
                continue;
            throw ErrnoException(ErrorCodes::CANNOT_POLL, "Cannot wait on socket (poll)");
        }

        int result = None;
        if (fd.revents & POLLIN)
            result |= Read;
        if (fd.revents & POLLOUT)
            result |= Write;
        if (fd.revents & (POLLERR | POLLHUP | POLLNVAL))
            result |= Error;
        return result;
    }
}

Int64 Socket::peek(char * buffer, size_t size) const
{
    ssize_t res = 0;
    do
        res = ::recv(handle, buffer, size, MSG_PEEK | MSG_DONTWAIT);
    while (res < 0 && isInterrupted(lastError()));
    return res;
}

int Socket::lastError()
{
    return errno;
}

bool Socket::isWouldBlock(int error)
{
    /// The two are the same value on Linux but need not be, and POSIX allows either.
    return error == EAGAIN || error == EWOULDBLOCK;
}

bool Socket::isInterrupted(int error)
{
    return error == EINTR;
}

#endif

}
