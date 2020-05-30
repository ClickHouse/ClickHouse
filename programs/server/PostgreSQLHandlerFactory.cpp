#include "PostgreSQLHandlerFactory.h"
#include "IServer.h"
#include <Poco/Net/TCPServerConnectionFactory.h>
#include <memory>
#include "PostgreSQLHandler.h"

namespace DB
{

PostgreSQLHandlerFactory::PostgreSQLHandlerFactory(IServer & server_)
    : server(server_)
    , log(&Logger::get("PostgreSQLHandlerFactory"))
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
    LOG_TRACE(log, "PostgreSQL connection. Id: " << connection_id << ". Address: " << socket.peerAddress().toString());
    return new PostgreSQLHandler(socket, server, ssl_enabled, connection_id, auth_methods);
}

}
