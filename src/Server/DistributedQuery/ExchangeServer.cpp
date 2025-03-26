#include <Server/DistributedQuery/ExchangeServer.h>
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromPocoSocketChunked.h>
#include <IO/ReadHelpers.h>
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
}

ExchangeServer::ExchangeServer(UInt16 port, ExchangeConnectionsPtr connections_)
    : connections(std::move(connections_))
    , server_socket(Poco::Net::ServerSocket(port))
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
    LOG_DEBUG(log, "Starting ExchangeServer on port {}", server_socket.address().port());
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

void ExchangeServer::addConnection(Poco::Net::StreamSocket socket)
{
    LOG_TRACE(log, "Connection from {}", socket.peerAddress().toString());

    ReadBufferFromPocoSocketChunked in(socket);

    UInt64 packet_type = 0;
    readVarUInt(packet_type, in);
    if (packet_type != Protocol::Client::Hello)
        throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Unexpected packet type {}", packet_type);

    String query_id;
    readStringBinary(query_id, in);
    String stream_name;
    readStringBinary(stream_name, in);

    LOG_TRACE(log, "Query id: {}, stream: {}", query_id, stream_name);

    /// Send Hello back to finish handshake
    WriteBufferFromPocoSocketChunked out(socket);
    writeVarUInt(Protocol::Client::Hello, out);
    out.next();
    out.cancel();

    connections->addConnection(query_id, stream_name, socket);
}

}
