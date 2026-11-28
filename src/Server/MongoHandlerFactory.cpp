#include <Server/MongoHandlerFactory.h>

/// The Mongo wire protocol needs BSON (mongo-cxx-driver) and the Mongo dialect (rapidjson).
#if USE_MONGODB && USE_RAPIDJSON

#include <Server/MongoHandler.h>

#include <Common/logger_useful.h>

namespace DB
{

MongoHandlerFactory::MongoHandlerFactory(
    IServer & server_, bool secure_, const ProfileEvents::Event & read_event_, const ProfileEvents::Event & write_event_)
    : server(server_), log(getLogger("MongoHandlerFactory")), secure(secure_), read_event(read_event_), write_event(write_event_)
{
}

Poco::Net::TCPServerConnection * MongoHandlerFactory::createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    Int32 connection_id = last_connection_id++;
    LOG_TRACE(log, "Mongo connection{}. Id: {}. Address: {}", secure ? " (TLS)" : "", connection_id, socket.peerAddress().toString());

    return new MongoHandler(socket, server, tcp_server, secure, connection_id, read_event, write_event);
}

}

#endif
