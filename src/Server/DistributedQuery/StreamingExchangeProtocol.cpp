#include <Server/DistributedQuery/StreamingExchangeProtocol.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/StreamSocket.h>

#include <algorithm>
#include <climits>
#include <cerrno>

namespace DB
{

namespace ErrorCodes
{
    extern const int EXCHANGE_PEER_DISCONNECTED;
}

namespace StreamingExchangeProtocol
{

UInt64 SourceHelloBody::readVersion(ReadBuffer & in)
{
    UInt64 version = 0;
    readIntBinary(version, in);
    return version;
}

void SourceHelloBody::readAfterVersion(ReadBuffer & in)
{
    readStringBinary(query_id, in);
    readStringBinary(stream_name, in);
    readStringBinary(auth_token, in);
}

void SourceHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(source_version, out);
    writeStringBinary(query_id, out);
    writeStringBinary(stream_name, out);
    writeStringBinary(auth_token, out);
}

void SinkHelloBody::read(ReadBuffer & in)
{
    readIntBinary(sink_version, in);
}

void SinkHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(sink_version, out);
}

namespace
{
    /// The errors `recv` and `send` report when the other side of an established connection is gone.
    bool isPeerGoneError(int socket_errno)
    {
        return socket_errno == ECONNRESET || socket_errno == ECONNABORTED || socket_errno == EPIPE
            || socket_errno == ENETRESET || socket_errno == ENOTCONN || socket_errno == ETIMEDOUT;
    }
}

String describePeer(const Poco::Net::StreamSocket & socket)
{
    try
    {
        return socket.peerAddress().toString();
    }
    catch (const Poco::Exception &)
    {
        return "unknown peer";
    }
}

void throwSocketError(int socket_errno, const Poco::Net::StreamSocket & socket, const String & what)
{
    if (isPeerGoneError(socket_errno))
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}), errno {}", what, describePeer(socket), socket_errno);
    throw Poco::Net::NetException(fmt::format("Failed to {} ({}), errno {}", what, describePeer(socket), socket_errno));
}

void rethrowSocketException(const Poco::Net::StreamSocket & socket, const String & what)
{
    try
    {
        throw;
    }
    catch (const Poco::IOException & e)
    {
        if (!isPeerGoneError(e.code()))
            throw;
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}): {}", what, describePeer(socket), e.displayText());
    }
    catch (const Poco::TimeoutException & e)
    {
        /// The kernel's connection timeout (`ETIMEDOUT`: the peer stopped answering) comes as a timeout
        /// exception carrying that errno. A deadline that ran out carries `EAGAIN` and stays a timeout.
        if (e.code() != ETIMEDOUT)
            throw;
        throw Exception(ErrorCodes::EXCHANGE_PEER_DISCONNECTED, "Failed to {} ({}), connection timed out", what, describePeer(socket));
    }
}

ssize_t tryReceive(Poco::Net::StreamSocket & socket, char * buffer, size_t size, const String & description)
{
    /// Poco's receiveBytes takes int. Cap the request at INT_MAX so a >2 GiB buffer
    /// (the data-path body is sized by an untrusted peer) does not wrap negative.
    const int chunk = static_cast<int>(std::min<size_t>(size, INT_MAX));
    while (true)
    {
        ssize_t received = 0;
        try
        {
            received = socket.receiveBytes(buffer, chunk);
        }
        catch (const Poco::Exception &)
        {
            rethrowSocketException(socket, "receive " + description);
        }
        if (received > 0)
            return received;
        if (received == 0)
            return -1;

        const int last_error = errno;
        if (last_error == EINTR)
            continue;
        if (last_error == EAGAIN || last_error == EWOULDBLOCK)
            return 0;
        throwSocketError(last_error, socket, "receive " + description);
    }
}

void sendAll(Poco::Net::StreamSocket & socket, const char * buffer, size_t size, const String & description)
{
    size_t position = 0;
    while (position < size)
    {
        const int chunk = static_cast<int>(std::min<size_t>(size - position, INT_MAX));
        int sent = 0;
        try
        {
            sent = socket.sendBytes(buffer + position, chunk);
        }
        catch (const Poco::Exception &)
        {
            rethrowSocketException(socket, "send " + description);
        }
        if (sent >= 0)
        {
            position += sent;
            continue;
        }

        const int last_error = errno;
        if (last_error == EINTR)
            continue;
        throwSocketError(last_error, socket, "send " + description);
    }
}

}
}
