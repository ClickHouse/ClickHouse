#pragma once

#include "config.h"

namespace Poco::Net { class StreamSocket; }

#if USE_SSL
struct ssl_st;
#endif

namespace DB
{

/// The result of a non-blocking, non-destructive probe of a connected socket.
enum class SocketState
{
    /// The connection is alive and there is no unread data: safe to reuse from a connection pool.
    Idle,
    /// The connection is alive, but unread data is waiting. For a TLS socket this means
    /// application data; TLS-internal records (a session ticket, `KeyUpdate`) do not count.
    /// An idle pooled connection must have nothing to read, so such a connection is not safe
    /// to reuse: the unread bytes are an unsolicited response (e.g. a queued `408 Request Timeout`
    /// a server sends before closing an idle connection) that the next request would misparse.
    DataPending,
    /// The peer closed the connection (a FIN or a TLS `close_notify`) or it is broken.
    Closed,
};

/// Probes the raw file descriptor with a single `recv(..., MSG_PEEK | MSG_DONTWAIT)`:
///   - `> 0`               -> `DataPending` (bytes are waiting to be read);
///   - `0`                 -> `Closed` (the peer sent a FIN, the next read would return EOF);
///   - `EAGAIN`            -> `Idle` (a healthy idle connection);
///   - any other error     -> `Closed` (e.g. RST -> `ECONNRESET`).
///
/// This is a raw (TLS-unaware) check on the plain socket; on a TLS connection use the
/// `StreamSocket` overload below, which is TLS-aware.
SocketState getSocketState(int fd);

/// Same as above for a Poco socket.
///
/// For a plain socket this checks the underlying file descriptor. For a TLS (`SecureStreamSocket`)
/// socket it uses `SSL_peek` instead, because a raw `MSG_PEEK` cannot tell TLS records apart:
///   - a `poll(SELECT_READ)` + `available()` (`SSL_pending`) check misfires on an unread TLS
///     post-handshake record (a session ticket or `KeyUpdate`), reporting a live idle connection
///     as readable, i.e. not reusable;
///   - a raw `MSG_PEEK` would misfire the other way, reporting an unread `close_notify` (an orderly
///     TLS shutdown) as pending data on a live connection, so a pool would hand out a dead one.
/// `SSL_peek` decrypts just enough to distinguish real application data (`DataPending`) and
/// harmless post-handshake messages (`Idle`) from a `close_notify` (`Closed`), without consuming
/// application data.
SocketState getSocketState(const Poco::Net::StreamSocket & socket);

#if USE_SSL
/// TLS-aware core of the check, exposed for testing. The socket underlying `ssl` MUST be in
/// non-blocking mode (the caller guarantees this) so that `SSL_peek` cannot block.
SocketState getSslSocketState(ssl_st * ssl);
#endif

/// Non-blocking, non-destructive check of whether the remote peer has closed the connection:
/// `getSocketState(...) == SocketState::Closed`. A live connection returns false whether or not
/// there are bytes waiting to be read; for a connection-pool reuse check, which must also reject
/// a connection with unread data, test for `SocketState::Idle` instead.
bool isSocketPeerClosed(int fd);

/// Same as above for a Poco socket (TLS-aware, see `getSocketState`).
bool isSocketPeerClosed(const Poco::Net::StreamSocket & socket);

#if USE_SSL
/// Same as above for an OpenSSL connection (see `getSslSocketState`).
bool isSslPeerClosed(ssl_st * ssl);
#endif

}
