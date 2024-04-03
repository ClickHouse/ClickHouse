#include "PostgreSQLHandlerFactory.h"
#include <memory>
#include <Server/PostgreSQLHandler.h>

namespace DB
{

PostgreSQLHandlerFactory::PostgreSQLHandlerFactory(IServer & server_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : server(server_)
    , log(getLogger("PostgreSQLHandlerFactory"))
    , read_event(read_event_)
    , write_event(write_event_)
{
    auth_methods =
    {
        std::make_shared<PostgreSQLProtocol::PGAuthentication::NoPasswordAuth>(),
        std::make_shared<PostgreSQLProtocol::PGAuthentication::CleartextPasswordAuth>(),
    };
}

Poco::Net::TCPServerConnection * PostgreSQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "PostgreSQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
    return new PostgreSQLHandler(socket, server, tcp_server, ssl_enabled, connection_id, auth_methods, read_event, write_event);
}

}
