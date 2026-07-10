#include <Server/DistributedQuery/StreamingExchangeProtocol.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/StreamSocket.h>

#include <algorithm>
#include <climits>
#include <cerrno>

namespace DB
{
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
}

void SourceHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(source_version, out);
    writeStringBinary(query_id, out);
    writeStringBinary(stream_name, out);
}

void SinkHelloBody::read(ReadBuffer & in)
{
    readIntBinary(sink_version, in);
}

void SinkHelloBody::write(WriteBuffer & out) const
{
    writeIntBinary(sink_version, out);
}

ssize_t tryReceive(Poco::Net::StreamSocket & socket, char * buffer, size_t size, const String & description)
{
    /// Poco's receiveBytes takes int. Cap the request at INT_MAX so a >2 GiB buffer
    /// (the data-path body is sized by an untrusted peer) does not wrap negative.
    const int chunk = static_cast<int>(std::min<size_t>(size, INT_MAX));
    while (true)
    {
        ssize_t received = socket.receiveBytes(buffer, chunk);
        if (received > 0)
            return received;
        if (received == 0)
            throw Poco::Net::NetException(fmt::format(
                "Failed to receive {} from {}, peer closed connection", description, socket.peerAddress().toString()));

        const int last_error = errno;
        if (last_error == EINTR)
            continue;
        if (last_error == EAGAIN || last_error == EWOULDBLOCK)
            return 0;
        throw Poco::Net::NetException(fmt::format(
            "Failed to receive {} from {}, errno {}", description, socket.peerAddress().toString(), last_error));
    }
}

}
}
