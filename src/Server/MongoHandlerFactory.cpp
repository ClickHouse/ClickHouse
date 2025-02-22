#include "MongoHandlerFactory.h"
#include <memory>
#include <Server/MongoHandler.h>

#include <iostream>

namespace DB
{

MongoHandlerFactory::MongoHandlerFactory(
    IServer & server_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : server(server_), log(getLogger("MongoHandlerFactory")), read_event(read_event_), write_event(write_event_)
{
    std::cerr << "mongo constructor\n";
}

Poco::Net::TCPServerConnection * MongoHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    std::cerr << "mongo connection\n";
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "Mongo connection. Id: {}. Address: {}", connection_id, socket.peerAddress().toString());

    return new MongoHandler(socket, server, tcp_server, false, connection_id, read_event, write_event);
}

}
