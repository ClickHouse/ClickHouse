#include <Poco/Net/StreamSocket.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <Server/RedisHandler.h>
#include <Server/RedisHandlerFactory.h>
#include <Server/RedisProtocolMapping.h>
#include <Common/logger_useful.h>

namespace DB
{

RedisHandlerFactory::RedisHandlerFactory(IServer & server_)
    : server(server_), log(getLogger("RedisHandlerFactory"))
{
    /// Validate the configuration at startup, so that a mistake in it is reported immediately and not
    /// on the first Redis request. The mapping itself is not cached here: it is read from the live
    /// configuration for every request, so that `SYSTEM RELOAD CONFIG` takes effect without a restart
    /// (the listener is not restarted unless the host or the port changes).
    RedisProtocol::parseConfig(server.config());
}

Poco::Net::TCPServerConnection * RedisHandlerFactory::createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    LOG_TRACE(log, "Redis connection. Address: {}", socket.peerAddress().toString());
    return new RedisHandler(server, tcp_server, socket);
}

}
