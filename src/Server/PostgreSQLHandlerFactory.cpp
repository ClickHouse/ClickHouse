#include "PostgreSQLHandlerFactory.h"
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <memory>
#include <Server/PostgreSQLHandler.h>

namespace DB
{

PostgreSQLHandlerFactory::PostgreSQLHandlerFactory(IServer & server_)
    : server(server_)
    , log(&Poco::Logger::get("PostgreSQLHandlerFactory"))
{
    auth_methods =
    {
        std::make_shared<PostgreSQLProtocol::PGAuthentication::NoPasswordAuth>(),
        std::make_shared<PostgreSQLProtocol::PGAuthentication::CleartextPasswordAuth>(),
    };
}

Poco::Net::TCPServerConnection * PostgreSQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "PostgreSQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
    return new PostgreSQLHandler(socket, server, ssl_enabled, connection_id, auth_methods);
}

}
