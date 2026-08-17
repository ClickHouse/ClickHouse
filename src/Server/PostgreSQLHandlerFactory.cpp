#include <Server/PostgreSQLHandlerFactory.h>
#include <memory>
#include <Server/PostgreSQLHandler.h>
#include <Core/PostgreSQLProtocol.h>

namespace DB
{

PostgreSQLHandlerFactory::PostgreSQLHandlerFactory(
    IServer & server_,
    bool secure_required_,
#if USE_SSL
    const std::string & conf_name_,
#endif
    const ProfileEvents::Event & read_event_,
    const ProfileEvents::Event & write_event_)
    : server(server_)
    , log(getLogger("PostgreSQLHandlerFactory"))
    , read_event(read_event_)
    , write_event(write_event_)
#if USE_SSL
    , conf_name(conf_name_)
#endif
    , secure_required(secure_required_)
{
    auth_methods =
    {
        std::make_shared<PostgreSQLProtocol::PGAuthentication::NoPasswordAuth>(),
        std::make_shared<PostgreSQLProtocol::PGAuthentication::CleartextPasswordAuth>(),
        std::make_shared<PostgreSQLProtocol::PGAuthentication::ScrambleSHA256Auth>(),
    };
}

Poco::Net::TCPServerConnection * PostgreSQLHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "PostgreSQL connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());

#if USE_SSL
    return new PostgreSQLHandler(socket, conf_name, server, tcp_server, ssl_enabled, secure_required, connection_id, auth_methods, read_event, write_event);
#else
    return new PostgreSQLHandler(socket, server, tcp_server, ssl_enabled, secure_required, connection_id, auth_methods, read_event, write_event);
#endif
}

}
