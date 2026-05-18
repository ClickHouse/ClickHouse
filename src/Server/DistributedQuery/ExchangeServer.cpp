#ifdef OS_LINUX
#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/StreamingExchangeProtocol.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Net/NetException.h>

#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int PROTOCOL_VERSION_MISMATCH;
}

ExchangeServer::ExchangeServer(const String & listen_host, UInt16 port, ExchangeConnectionsPtr connections_)
    : connections(std::move(connections_))
    , server_socket(Poco::Net::ServerSocket(Poco::Net::SocketAddress(listen_host, port)))
    , accept_thread("ExchangeServer")
    , stopped(true)
    , log(getLogger("ExchangeServer"))
{
}

ExchangeServer::~ExchangeServer()
{
    try
    {
        stop();
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}


void ExchangeServer::start()
{
    LOG_DEBUG(log, "Starting ExchangeServer on {}", server_socket.address().toString());
    stopped = false;
    accept_thread.start(*this);
}


void ExchangeServer::stop()
{
    if (!stopped)
    {
        stopped = true;
        accept_thread.join();
    }
}

void ExchangeServer::run()
{
    while (!stopped)
    {
        Poco::Timespan timeout(250000);
        try
        {
            if (server_socket.poll(timeout, Poco::Net::Socket::SELECT_READ))
            {
                try
                {
                    Poco::Net::StreamSocket ss = server_socket.acceptConnection();
                    addConnection(ss);
                }
                // Termination request
                catch (Poco::InvalidArgumentException &)
                {
                    break;
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                }
            }
        }
        catch (Poco::Exception &)
        {
            tryLogCurrentException(log);
            Poco::Thread::sleep(50);
        }
    }
}

namespace
{
    /// Read exactly `size` bytes from a blocking socket. Throws on EOF or transport error.
    void receiveAll(Poco::Net::StreamSocket & socket, void * buffer, size_t size, const String & context)
    {
        char * dst = static_cast<char *>(buffer);
        size_t position = 0;
        while (position < size)
        {
            size_t remaining = size - position;
            ssize_t received = socket.receiveBytes(dst + position, static_cast<int>(remaining));
            if (received < 0)
            {
                auto last_error = errno;
                if (last_error == EINTR)
                    continue;
                throw Poco::Net::NetException(fmt::format(
                    "Failed to receive {} from {}, errno {}", context, socket.peerAddress().toString(), last_error));
            }
            if (received == 0)
                throw Poco::Net::NetException(fmt::format(
                    "Failed to receive {} from {}, peer closed connection after {} of {} bytes",
                    context, socket.peerAddress().toString(), position, size));
            position += received;
        }
    }
}

void ExchangeServer::addConnection(Poco::Net::StreamSocket socket)
{
    LOG_TRACE(log, "Connection from {}", socket.peerAddress().toString());

    StreamingExchangeProtocol::PacketHeader header{};
    receiveAll(socket, &header, sizeof(header), "SourceHello header");

    if (header.packet_type != StreamingExchangeProtocol::PacketType::SourceHello)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "Unexpected packet type 0x{:x} from {} (expected SourceHello 0x{:x})",
            header.packet_type, socket.peerAddress().toString(),
            static_cast<UInt64>(StreamingExchangeProtocol::PacketType::SourceHello));

    if (header.bytes_size > StreamingExchangeProtocol::MAX_HELLO_BODY_BYTES)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "SourceHello body size {} from {} exceeds the limit {}",
            header.bytes_size, socket.peerAddress().toString(),
            StreamingExchangeProtocol::MAX_HELLO_BODY_BYTES);

    if (header.bytes_size < sizeof(UInt64))
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
            "SourceHello body size {} from {} is too small to contain the protocol version",
            header.bytes_size, socket.peerAddress().toString());

    std::vector<char> body_buffer(header.bytes_size);
    if (!body_buffer.empty())
        receiveAll(socket, body_buffer.data(), body_buffer.size(), "SourceHello body");

    ReadBufferFromMemory body_in(body_buffer.data(), body_buffer.size());
    UInt64 source_version = 0;
    readIntBinary(source_version, body_in);
    String query_id;
    readStringBinary(query_id, body_in);
    String stream_name;
    readStringBinary(stream_name, body_in);

    LOG_TRACE(log, "Query id: {}, stream: {}, peer protocol version: {}", query_id, stream_name, source_version);

    /// Send SinkHello before validating peer version so the peer can produce a precise diagnostic.
    /// Write failures are swallowed so the local mismatch throw below still wins.
    try
    {
        WriteBufferFromOwnString reply_body;
        writeIntBinary(StreamingExchangeProtocol::PROTOCOL_VERSION, reply_body);
        reply_body.finalize();
        const std::string & reply_body_str = reply_body.str();

        StreamingExchangeProtocol::PacketHeader reply_header{
            .packet_type = StreamingExchangeProtocol::PacketType::SinkHello,
            .bytes_size = reply_body_str.size(),
        };

        WriteBufferFromPocoSocket out(socket);
        out.write(reinterpret_cast<const char *>(&reply_header), sizeof(reply_header));
        if (!reply_body_str.empty())
            out.write(reply_body_str.data(), reply_body_str.size());
        out.next();
        out.cancel();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to send SinkHello to {}", socket.peerAddress().toString()));
    }

    if (source_version != StreamingExchangeProtocol::PROTOCOL_VERSION)
        throw Exception(ErrorCodes::PROTOCOL_VERSION_MISMATCH,
            "Streaming exchange protocol version mismatch from {} for stream {}: peer speaks version {}, this node speaks version {}",
            socket.peerAddress().toString(), stream_name, source_version,
            StreamingExchangeProtocol::PROTOCOL_VERSION);

    connections->addConnection(query_id, stream_name, socket);
}

}
#endif
