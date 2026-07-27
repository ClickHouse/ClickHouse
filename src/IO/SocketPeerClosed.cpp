#include <IO/SocketPeerClosed.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketImpl.h>

#if defined(OS_WINDOWS)
#include <Poco/UnWindows.h>
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif
#include <cerrno>

#if USE_SSL
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <fcntl.h>
#endif

namespace DB
{

namespace
{

/// `AsyncCallback` and the rest of the codebase pass socket descriptors around as `int`, but a
/// Windows `SOCKET` is pointer-sized and is not a file descriptor at all. Real socket handles
/// are small kernel handle values that fit in 32 bits, so this round trip is lossless in
/// practice - but making the socket-handle type portable throughout is the proper fix, and it
/// has to happen before the async event loop (which polls these descriptors) can work here.
int toSocketDescriptor(poco_socket_t handle)
{
#if defined(OS_WINDOWS)
    return static_cast<int>(static_cast<unsigned int>(handle));
#else
    return handle;
#endif
}

#if defined(OS_WINDOWS)
SOCKET toSocketHandle(int fd)
{
    return static_cast<SOCKET>(static_cast<unsigned int>(fd));
}
#endif

}

SocketState getSocketState(int fd)
{
    if (fd < 0)
        return SocketState::Closed;

#if defined(OS_WINDOWS)
    /// Winsock has no per-call `MSG_DONTWAIT` - whether a call blocks is a property of the
    /// socket, set with `ioctlsocket(FIONBIO)`, and this probe must not change that for a socket
    /// it does not own. So ask first, with a zero timeout, whether anything is readable: if
    /// nothing is, that is precisely the healthy idle case `EAGAIN` reports below, and if
    /// something is, the `MSG_PEEK` that follows cannot block.
    ///
    const auto handle = toSocketHandle(fd);

    fd_set readable;
    FD_ZERO(&readable);
    FD_SET(handle, &readable);
    /// The first argument is ignored on Windows; the sets carry the handles.
    timeval no_wait{.tv_sec = 0, .tv_usec = 0};
    const int ready = ::select(0, &readable, nullptr, nullptr, &no_wait);
    if (ready == 0)
        return SocketState::Idle;           /// Nothing to read and no FIN: a healthy idle connection.
    if (ready == SOCKET_ERROR)
        return SocketState::Closed;

    char c = 0;
    const int res = ::recv(handle, &c, 1, MSG_PEEK);
    if (res > 0)
        return SocketState::DataPending;    /// Bytes are waiting to be read; the peer is alive.
    if (res == 0)
        return SocketState::Closed;         /// Orderly shutdown: the peer sent a FIN, the next read would return EOF.

    /// Winsock reports through `WSAGetLastError`, not `errno`.
    const int error = ::WSAGetLastError();
    if (error == WSAEWOULDBLOCK)
        return SocketState::Idle;
    return SocketState::Closed;             /// Any other error (e.g. WSAECONNRESET): closed/broken.
#else
    char c = 0;
    ssize_t res = 0;
    do
        res = ::recv(fd, &c, 1, MSG_PEEK | MSG_DONTWAIT);
    while (res < 0 && errno == EINTR);

    if (res > 0)
        return SocketState::DataPending;    /// Bytes are waiting to be read; the peer is alive.
    if (res == 0)
        return SocketState::Closed;         /// Orderly shutdown: the peer sent a FIN, the next read would return EOF.

    /// res < 0
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return SocketState::Idle;           /// Nothing to read and no FIN: a healthy idle connection.
    return SocketState::Closed;             /// Any other error (e.g. ECONNRESET): treat as closed/broken.
#endif
}

#if USE_SSL

SocketState getSSLSocketState(ssl_st * ssl)
{
    /// `SSL_peek` decrypts just enough of the pending records to tell real application data and
    /// harmless post-handshake messages (session tickets, `KeyUpdate`) apart from a `close_notify`.
    /// It does not consume application data. The socket is non-blocking, so this never blocks.
    ///
    /// The error queue must be empty before the call for `SSL_get_error` to be meaningful.
    ERR_clear_error();
    char c = 0;
    int res = SSL_peek(ssl, &c, 1);
    if (res > 0)
        return SocketState::DataPending;    /// Application data is waiting to be read; the peer is alive.

    switch (SSL_get_error(ssl, res))
    {
        case SSL_ERROR_WANT_READ:  [[fallthrough]];
        case SSL_ERROR_WANT_WRITE:
            /// `SSL_peek` found no complete application-data record, but that alone does not prove
            /// the connection is idle: the bytes of a record that has only partially arrived (e.g.
            /// the first fragment of a queued response) are buffered inside the SSL object too, and
            /// look identical from here - both end in `SSL_ERROR_WANT_READ`. `SSL_has_pending`
            /// reports on that internal buffer regardless of whether the record is complete, so a
            /// session ticket / `KeyUpdate` that was fully consumed reads as idle (nothing left
            /// buffered), while a partial record correctly reads as pending.
            return SSL_has_pending(ssl) ? SocketState::DataPending : SocketState::Idle;
        case SSL_ERROR_ZERO_RETURN:
            return SocketState::Closed;     /// The peer sent `close_notify`: an orderly TLS shutdown.
        default:
            /// A FIN without `close_notify` (`SSL_ERROR_SYSCALL`), a protocol error (`SSL_ERROR_SSL`),
            /// or anything else: treat as closed/broken.
            return SocketState::Closed;
    }
}

namespace
{

/// Force the socket into non-blocking mode for the duration of a call, restoring the original
/// mode afterwards, so that `SSL_peek` on an idle pooled connection can never block.
///
/// Goes through Poco rather than `fcntl` because Winsock has no `fcntl`, and its
/// `ioctlsocket(FIONBIO)` can only set the mode, never read it back - Poco is the thing that
/// remembers what the mode was.
class ScopedNonBlocking
{
public:
    explicit ScopedNonBlocking(Poco::Net::SocketImpl & socket_) : socket(socket_), was_blocking(socket_.getBlocking())
    {
        if (was_blocking)
            socket.setBlocking(false);
    }

    ~ScopedNonBlocking()
    {
        try
        {
            if (was_blocking)
                socket.setBlocking(true);
        }
        catch (...) /// NOLINT(bugprone-empty-catch)
        {
            /// `setBlocking` throws on a socket that has since been closed; there is nothing to
            /// restore in that case, and a destructor must not propagate.
        }
    }

    ScopedNonBlocking(const ScopedNonBlocking &) = delete;
    ScopedNonBlocking & operator=(const ScopedNonBlocking &) = delete;

private:
    Poco::Net::SocketImpl & socket;
    bool was_blocking;
};

}

#endif

SocketState getSocketState(const Poco::Net::StreamSocket & socket)
{
#if USE_SSL
    if (auto * secure = dynamic_cast<Poco::Net::SecureStreamSocketImpl *>(socket.impl()))
    {
        /// A connected secure socket has a live `SSL` object; the null case (handshake not yet
        /// performed) has no TLS state to inspect, so fall back to the raw file-descriptor check.
        if (auto * ssl = secure->ssl())
        {
            ScopedNonBlocking non_blocking(*secure);
            return getSSLSocketState(ssl);
        }
    }
#endif
    return getSocketState(toSocketDescriptor(socket.impl()->sockfd()));
}

bool isSocketPeerClosed(int fd)
{
    return getSocketState(fd) == SocketState::Closed;
}

bool isSocketPeerClosed(const Poco::Net::StreamSocket & socket)
{
    return getSocketState(socket) == SocketState::Closed;
}

#if USE_SSL

bool isSSLPeerClosed(ssl_st * ssl)
{
    return getSSLSocketState(ssl) == SocketState::Closed;
}

#endif

}
