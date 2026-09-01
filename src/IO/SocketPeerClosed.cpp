#include <IO/SocketPeerClosed.h>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Net/SocketImpl.h>

#include <sys/socket.h>
#include <cerrno>

#if USE_SSL
#include <Poco/Net/SecureStreamSocketImpl.h>
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <fcntl.h>
#endif

namespace DB
{

SocketState getSocketState(int fd)
{
    if (fd < 0)
        return SocketState::Closed;

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
}

#if USE_SSL

SocketState getSslSocketState(ssl_st * ssl)
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
/// flags afterwards, so that `SSL_peek` on an idle pooled connection can never block.
class ScopedNonBlocking
{
public:
    explicit ScopedNonBlocking(int fd_) : fd(fd_), flags(::fcntl(fd_, F_GETFL, 0))
    {
        if (flags >= 0 && !(flags & O_NONBLOCK))
            ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }

    ~ScopedNonBlocking()
    {
        if (flags >= 0 && !(flags & O_NONBLOCK))
            ::fcntl(fd, F_SETFL, flags);
    }

    ScopedNonBlocking(const ScopedNonBlocking &) = delete;
    ScopedNonBlocking & operator=(const ScopedNonBlocking &) = delete;

private:
    int fd;
    int flags;
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
            ScopedNonBlocking non_blocking(secure->sockfd());
            return getSslSocketState(ssl);
        }
    }
#endif
    return getSocketState(socket.impl()->sockfd());
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

bool isSslPeerClosed(ssl_st * ssl)
{
    return getSslSocketState(ssl) == SocketState::Closed;
}

#endif

}
