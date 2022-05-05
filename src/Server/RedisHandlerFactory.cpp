#include "RedisHandlerFactory.h"
#include "RedisHandler.h"

namespace DB
{

RedisHandlerFactory::RedisHandlerFactory(IServer & server_) : server(server_), log(&Poco::Logger::get("RedisHandlerFactory"))
{
}

Poco::Net::TCPServerConnection * RedisHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    LOG_TRACE(log, "Redis connection. Address: {}", socket.peerAddress().toString());
    return new RedisHandler(socket, server, tcp_server);
}

}
