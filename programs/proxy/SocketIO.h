#pragma once

#include "config.h"

#if USE_SILK

#include <base/types.h>

#include <Poco/Net/SocketAddress.h>
#include <Poco/Net/StreamSocket.h>

#if USE_SSL
#include <Poco/Net/Context.h>
#endif

namespace DB::Proxy
{

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
#endif

    /// Returns the number of bytes read, or 0 on end of stream.
    int receive(char * buffer, int length);

    /// Sends the whole buffer. Throws on failure.
    void sendAll(const char * buffer, size_t length);

    void setTimeouts(UInt64 receive_ms, UInt64 send_ms);
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
