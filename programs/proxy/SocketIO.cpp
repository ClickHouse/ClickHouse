#include <SocketIO.h>

#if USE_SILK

#include <IO/SilkFiberStreamSocketImpl.h>

#include <Common/Exception.h>

#include <Poco/Timespan.h>

#if USE_SSL
#include <IO/SilkSecureFiberStreamSocketImpl.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
}
}

namespace DB::Proxy
{

static Poco::Timespan ms(UInt64 milliseconds)
{
    return Poco::Timespan(static_cast<Poco::Timespan::TimeDiff>(milliseconds) * 1000);
}

FiberSocket FiberSocket::adopt(int fd)
{
    FiberSocket result;
    result.socket = Poco::Net::StreamSocket(new Silk::FiberStreamSocketImpl(fd));
    /// Disable Nagle's algorithm: the proxy relays small protocol packets, and without this a
    /// request/response round-trip stalls for tens of milliseconds on Nagle/delayed-ACK.
    result.socket.setNoDelay(true);
    return result;
}

FiberSocket FiberSocket::connect(const Poco::Net::SocketAddress & address, UInt64 timeout_ms)
{
    FiberSocket result;
    result.socket = Poco::Net::StreamSocket(new Silk::FiberStreamSocketImpl);
    result.socket.connect(address, ms(timeout_ms));
    result.socket.setNoDelay(true);
    return result;
}

#if USE_SSL
FiberSocket FiberSocket::connectTLS(
    const Poco::Net::SocketAddress & address, UInt64 timeout_ms,
    Poco::Net::Context::Ptr context, const String & sni)
{
    FiberSocket result;
    auto * impl = new Silk::SecureFiberStreamSocketImpl(context);
    result.socket = Poco::Net::StreamSocket(impl);
    if (!sni.empty())
        impl->setPeerHostName(sni);
    result.socket.connect(address, ms(timeout_ms));
    result.socket.setNoDelay(true);
    result.is_plaintext = false;
    return result;
}

FiberSocket FiberSocket::adoptTLS(int fd, Poco::Net::Context::Ptr context)
{
    FiberSocket result;
    result.socket = Poco::Net::StreamSocket(new Silk::SecureFiberStreamSocketImpl(fd, context));
    result.socket.setNoDelay(true);
    result.is_plaintext = false;
    return result;
}
#endif

int FiberSocket::receive(char * buffer, int length)
{
    try
    {
        return socket.receiveBytes(buffer, length);
    }
    catch (const Poco::TimeoutException &)
    {
        throw Exception(ErrorCodes::SOCKET_TIMEOUT, "Timeout while reading from socket");
    }
    catch (const Poco::Exception & e)
    {
        throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot read from socket: {}", e.displayText());
    }
}

void FiberSocket::sendAll(const char * buffer, size_t length)
{
    size_t sent = 0;
    while (sent < length)
    {
        int n = 0;
        try
        {
            n = socket.sendBytes(buffer + sent, static_cast<int>(length - sent));
        }
        catch (const Poco::TimeoutException &)
        {
            throw Exception(ErrorCodes::SOCKET_TIMEOUT, "Timeout while writing to socket");
        }
        catch (const Poco::Exception & e)
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Cannot write to socket: {}", e.displayText());
        }
        if (n <= 0)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Connection closed while writing to socket");
        sent += n;
    }
}

void FiberSocket::setTimeouts(UInt64 receive_ms, UInt64 send_ms)
{
    socket.setReceiveTimeout(ms(receive_ms));
    socket.setSendTimeout(ms(send_ms));
}

void FiberSocket::close()
{
    try
    {
        if (socket.impl()->initialized())
            socket.close();
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        /// Closing a broken connection may throw; there is nothing to do about it.
    }
}

bool RecordingReader::ensure(size_t n)
{
    while (buffer.size() - pos < n)
    {
        char chunk[4096];
        int got = socket.receive(chunk, sizeof(chunk));
        if (got <= 0)
            return false;
        buffer.append(chunk, got);
    }
    return true;
}

UInt8 RecordingReader::readByte()
{
    if (!ensure(1))
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Unexpected end of stream while parsing a packet");
    return static_cast<UInt8>(buffer[pos++]);
}

UInt8 RecordingReader::peekByte()
{
    if (!ensure(1))
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Unexpected end of stream while parsing a packet");
    return static_cast<UInt8>(buffer[pos]);
}

void RecordingReader::readInto(char * dst, size_t n)
{
    if (!ensure(n))
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Unexpected end of stream while parsing a packet");
    memcpy(dst, buffer.data() + pos, n);
    pos += n;
}

void RecordingReader::skip(size_t n)
{
    if (!ensure(n))
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Unexpected end of stream while parsing a packet");
    pos += n;
}

template <typename T> T RecordingReader::readLE()
{
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value |= static_cast<T>(readByte()) << (8 * i);
    return value;
}

template <typename T> T RecordingReader::readBE()
{
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i)
        value = static_cast<T>(value << 8) | readByte();
    return value;
}

template UInt16 RecordingReader::readLE<UInt16>();
template UInt32 RecordingReader::readLE<UInt32>();
template UInt32 RecordingReader::readBE<UInt32>();

UInt64 RecordingReader::readVarUInt()
{
    UInt64 value = 0;
    for (size_t i = 0; i < 10; ++i)
    {
        UInt8 byte = readByte();
        value |= static_cast<UInt64>(byte & 0x7F) << (7 * i);
        if (!(byte & 0x80))
            return value;
    }
    throw Exception(ErrorCodes::NETWORK_ERROR, "Malformed variable-length integer");
}

String RecordingReader::readVarString()
{
    UInt64 length = readVarUInt();
    if (length > 64 * 1024)
        throw Exception(ErrorCodes::NETWORK_ERROR, "String in handshake is too long: {}", length);
    return readFixed(length);
}

String RecordingReader::readNullTerminated()
{
    String result;
    while (true)
    {
        UInt8 byte = readByte();
        if (byte == 0)
            break;
        result += static_cast<char>(byte);
        if (result.size() > 64 * 1024)
            throw Exception(ErrorCodes::NETWORK_ERROR, "String in handshake is too long");
    }
    return result;
}

String RecordingReader::readFixed(size_t n)
{
    if (!ensure(n))
        throw Exception(ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF, "Unexpected end of stream while parsing a packet");
    String result = buffer.substr(pos, n);
    pos += n;
    return result;
}

bool RecordingReader::readLine(String & line, size_t max_length)
{
    line.clear();
    while (true)
    {
        if (buffer.size() - pos == 0 && !ensure(1))
            return !line.empty();

        char c = buffer[pos++];
        if (c == '\n')
        {
            if (!line.empty() && line.back() == '\r')
                line.pop_back();
            return true;
        }
        line += c;
        if (line.size() > max_length)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Line is too long: exceeds {} bytes", max_length);
    }
}

}

#endif
