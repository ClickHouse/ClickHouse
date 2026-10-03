#include <Common/WakeupFd.h>

#include <Common/Exception.h>
#include <Common/NetException.h>

namespace DB
{

/// The declarations of both implementations below, in a single block: the style check counts them
/// per file and does not follow the preprocessor.
namespace ErrorCodes
{
    extern const int CANNOT_FSTAT;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

}

#if defined(OS_WINDOWS)

#include <Poco/UnWindows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

namespace DB
{

namespace
{

/// Two connected loopback TCP sockets, standing in for a pipe. `socketpair` does not exist on
/// Windows, and `AF_UNIX` sockets - which do exist on Windows 10 and later - cannot be made into a
/// `SOCK_STREAM` pair without a filesystem path; a loopback connection is what libuv, Asio and
/// CPython all fall back to.
void createLoopbackPair(Socket::Handle & read_end, Socket::Handle & write_end)
{
    const SOCKET listener = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET)
        throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot create a wakeup socket, error code: {}", Socket::lastError());

    SOCKET accepted = INVALID_SOCKET;
    SOCKET connected = INVALID_SOCKET;

    try
    {
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
        /// Port 0: let the system pick one, then read it back with `getsockname`.
        address.sin_port = 0;

        if (::bind(listener, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == SOCKET_ERROR
            || ::listen(listener, 1) == SOCKET_ERROR)
            throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot listen on a wakeup socket, error code: {}", Socket::lastError());

        int address_length = sizeof(address);
        if (::getsockname(listener, reinterpret_cast<sockaddr *>(&address), &address_length) == SOCKET_ERROR)
            throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot get the wakeup socket address, error code: {}", Socket::lastError());

        connected = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (connected == INVALID_SOCKET)
            throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot create a wakeup socket, error code: {}", Socket::lastError());

        /// Both sockets are blocking at this point, and the listener has a backlog of one, so this
        /// connect completes without waiting for the accept.
        if (::connect(connected, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) == SOCKET_ERROR)
            throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot connect a wakeup socket, error code: {}", Socket::lastError());

        accepted = ::accept(listener, nullptr, nullptr);
        if (accepted == INVALID_SOCKET)
            throw NetException(ErrorCodes::NETWORK_ERROR, "Cannot accept a wakeup socket, error code: {}", Socket::lastError());

        /// Non-blocking on both ends: `notify` must not block when the buffer is full (a wakeup is
        /// already pending) and `drain` must not block when it is empty.
        u_long non_blocking = 1;
        if (::ioctlsocket(accepted, FIONBIO, &non_blocking) == SOCKET_ERROR
            || ::ioctlsocket(connected, FIONBIO, &non_blocking) == SOCKET_ERROR)
            throw NetException(
                ErrorCodes::NETWORK_ERROR, "Cannot make a wakeup socket non-blocking, error code: {}", Socket::lastError());

        /// A wakeup is one byte and has to arrive now, not once Nagle has something to coalesce
        /// it with.
        const int no_delay = 1;
        ::setsockopt(connected, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<const char *>(&no_delay), sizeof(no_delay));
    }
    catch (...)
    {
        if (accepted != INVALID_SOCKET)
            ::closesocket(accepted);
        if (connected != INVALID_SOCKET)
            ::closesocket(connected);
        ::closesocket(listener);
        throw;
    }

    ::closesocket(listener);

    read_end = static_cast<Socket::Handle>(accepted);
    write_end = static_cast<Socket::Handle>(connected);
}

}

WakeupFd::WakeupFd()
{
    createLoopbackPair(read_end.handle, write_end.handle);
}

WakeupFd::~WakeupFd()
{
    if (write_end.isValid())
        ::closesocket(static_cast<SOCKET>(write_end.handle));
    if (read_end.isValid())
        ::closesocket(static_cast<SOCKET>(read_end.handle));
}

int WakeupFd::fd() const
{
    return read_end.toDescriptor();
}

void WakeupFd::notify() const
{
    const char byte = '\0';
    while (true)
    {
        const int r = ::send(static_cast<SOCKET>(write_end.handle), &byte, 1, 0);
        if (r == 1)
            return;

        const int error = Socket::lastError();
        if (Socket::isInterrupted(error))
            continue;
        if (Socket::isWouldBlock(error))
            return;   /// buffer full -> wakeup already pending
        throw NetException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write to wakeup socket, error code: {}", error);
    }
}

void WakeupFd::drain() const
{
    char buf[4096];
    while (true)
    {
        const int r = ::recv(static_cast<SOCKET>(read_end.handle), buf, sizeof(buf), 0);
        if (r > 0)
            continue;
        if (r == 0)
            return;   /// write end closed; nothing more to read

        const int error = Socket::lastError();
        if (Socket::isInterrupted(error))
            continue;
        if (Socket::isWouldBlock(error))
            return;
        throw NetException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot read from wakeup socket, error code: {}", error);
    }
}

}

#else

#include <Common/ErrnoException.h>

#include <cerrno>
#include <unistd.h>

#ifdef DEBUG_OR_SANITIZER_BUILD
#include <fcntl.h>
#include <sys/stat.h>
#endif

namespace DB
{

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

WakeupFd::~WakeupFd() = default;

int WakeupFd::fd() const
{
    return pipe.fds_rw[0];
}

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

#endif
