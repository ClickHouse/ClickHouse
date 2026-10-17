#include <Server/RedisHandlerFactory.h>

#include <Poco/Net/NetException.h>

#include <Server/RedisHandler.h>

namespace DB
{

namespace
{

class DummyTCPHandler : public Poco::Net::TCPServerConnection
{
public:
    using Poco::Net::TCPServerConnection::TCPServerConnection;
    void run() override {}
};

}

RedisHandlerFactory::RedisHandlerFactory(IServer & server_)
    : server(server_)
    , log(getLogger("RedisHandlerFactory"))
{
}

Poco::Net::TCPServerConnection * RedisHandlerFactory::createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    UInt64 connection_id = last_connection_id++;

    try
    {
        LOG_TRACE(log, "Redis connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
        return new RedisHandler(server, tcp_server, socket, connection_id);
    }
    catch (const Poco::Net::NetException &)
    {
        LOG_TRACE(log, "Redis connection. Client is not connected (most likely RST packet was sent).");
        return new DummyTCPHandler(socket);
    }
}

}
