#pragma once

#include "config.h"

#if USE_SILK

#include <base/types.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#include <silk/fibers/event.h>
#include <silk/fibers/future.h>

#include <memory>

#if USE_SSL
#include <Poco/Net/Context.h>
#endif

namespace DB::Proxy
{

/// Resolve a backend address. A host name is resolved outside the cooperative scheduler,
/// so that a slow DNS lookup does not stall the other fibers of the thread.
Poco::Net::SocketAddress resolveAddress(const String & host, UInt16 port);

/// A deadline for the whole handshake of an accepted connection. A socket receive timeout bounds every
/// single read, and each read that returns a few bytes starts it anew, so a client could drip-feed its
/// handshake forever and pin a connection fiber. A watchdog fiber shuts the connection down when
/// the deadline passes, which ends any pending read: of a `RecordingReader`, of a buffer over the raw
/// socket, or within the TLS handshake. `disarm` (or the destructor) stops and joins the watchdog,
/// so it never touches the descriptor after it may have been closed.
class HandshakeDeadline
{
public:
    /// Starts the watchdog fiber. Throws if it cannot be started.
    HandshakeDeadline(int fd_, UInt64 timeout_ms_);
    ~HandshakeDeadline() { disarm(); }

    HandshakeDeadline(const HandshakeDeadline &) = delete;
    HandshakeDeadline & operator=(const HandshakeDeadline &) = delete;

    void disarm() noexcept;

private:
    struct WatchdogTask
    {
        HandshakeDeadline * self;
    };
    static int watch(WatchdogTask * task) noexcept;

    const int fd;
    const UInt64 timeout_ms;
    silk::FiberEvent disarmed;
    silk::FiberFuture finished;
    bool joined = false;
};

/// A cooperative TCP endpoint backed by a silk fiber socket, optionally wrapped in TLS.
/// All calls suspend the current fiber instead of blocking the OS thread.
class FiberSocket
{
public:
    FiberSocket() = default;

    /// Adopt an accepted plaintext connection (owns the fd).
    static FiberSocket adopt(int fd);

    /// Connect to an address (plaintext). Throws on failure.
    static FiberSocket connect(const Poco::Net::SocketAddress & address, UInt64 timeout_ms);

#if USE_SSL
    /// Connect to an address and perform a client-side TLS handshake, sending @p sni as the server name.
    static FiberSocket connectTLS(
        const Poco::Net::SocketAddress & address, UInt64 timeout_ms,
        Poco::Net::Context::Ptr context, const String & sni);

    /// Adopt an accepted connection and terminate TLS on it (server-side handshake, silk fiber BIO).
    static FiberSocket adoptTLS(int fd, Poco::Net::Context::Ptr context);

    /// The server name (SNI) the client sent in the TLS handshake of a TLS-terminated socket,
    /// or an empty string. Only meaningful after the handshake, i.e. after the first read.
    String tlsServerName();
#endif

    /// Returns the number of bytes read, or 0 on end of stream.
    int receive(char * buffer, int length);

    /// Sends the whole buffer. Throws on failure.
    void sendAll(const char * buffer, size_t length);

    void setTimeouts(UInt64 receive_ms, UInt64 send_ms);

    /// Bound the whole handshake of an accepted connection by @p timeout_ms (see `HandshakeDeadline`).
    void armHandshakeDeadline(UInt64 timeout_ms);
    /// The handshake is over: the connection is no longer bounded by the handshake deadline.
    void disarmHandshakeDeadline() { handshake_deadline.reset(); }

    Poco::Net::SocketAddress peerAddress() const { return socket.peerAddress(); }
    void close();

    bool initialized() const { return !socket.impl()->initialized() ? false : true; }
    Poco::Net::StreamSocket & raw() { return socket; }
    int fd() { return socket.impl()->sockfd(); }

    /// True when the socket carries plaintext (no TLS termination on this leg). Only plaintext legs
    /// can be relayed with splice(2); a TLS-terminated stream must be decrypted in user space.
    bool plaintext() const { return is_plaintext; }

private:
    Poco::Net::StreamSocket socket;
    bool is_plaintext = true;
    int adopted_fd = -1;    /// The descriptor of an accepted connection.
    /// Declared after `socket`, so the watchdog is joined before the socket is destroyed.
    std::unique_ptr<HandshakeDeadline> handshake_deadline;
};

/// A buffered reader over a FiberSocket that keeps every byte it received, so the consumed
/// prefix (the handshake the proxy parsed) can be forwarded verbatim to the chosen backend.
class RecordingReader
{
public:
    explicit RecordingReader(FiberSocket & socket_) : socket(socket_) {}

    /// Ensure at least @p n unread bytes are buffered. Returns false on end of stream.
    bool ensure(size_t n);

    UInt8 readByte();
    UInt8 peekByte();
    void readInto(char * dst, size_t n);
    void skip(size_t n);

    /// Read a little-endian fixed-width unsigned integer.
    template <typename T> T readLE();
    /// Read a big-endian fixed-width unsigned integer.
    template <typename T> T readBE();

    UInt64 readVarUInt();
    String readVarString();                 /// Native protocol string: varint length then bytes.
    String readNullTerminated();            /// Bytes up to and excluding the next NUL (which is consumed).
    String readFixed(size_t n);

    /// Read one CRLF- or LF-terminated line without the terminator. Returns false on end of stream.
    bool readLine(String & line, size_t max_length);

    size_t position() const { return pos; }
    size_t buffered() const { return buffer.size() - pos; }

    /// All bytes received so far, to be forwarded to the backend.
    const String & received() const { return buffer; }

private:
    FiberSocket & socket;
    String buffer;
    size_t pos = 0;
};

}

#endif
