#include "MongoDBHandlerFactory.h"
#include <memory>
#include <Server/MongoDBHandler.h>
#include "Common/logger_useful.h"

namespace DB
{

MongoDBHandlerFactory::MongoDBHandlerFactory(
    IServer & server_)
    : server(server_), log(getLogger(factory_name))
{
    // auth_methods =
    // {
    //     std::make_shared<PostgreSQLProtocol::PGAuthentication::NoPasswordAuth>(),
    //     std::make_shared<PostgreSQLProtocol::PGAuthentication::CleartextPasswordAuth>(),
    // };
}

Poco::Net::TCPServerConnection * MongoDBHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "MongoDB connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());
    return new MongoDBHandler(socket, server, tcp_server, connection_id);
}

}
