#pragma once

#include <Core/PostgreSQLProtocol.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include "IServer.h"
#include "config.h"

#include <Core/Mongo/MongoProtocol.h>

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** MongoDB wire protocol implementation.
 * For more info see https://www.mongodb.com/docs/v5.0/reference/mongodb-wire-protocol
 */
class MongoHandler : public Poco::Net::TCPServerConnection
{
public:
    MongoHandler(
        const Poco::Net::StreamSocket & socket_,
        IServer & server_,
        TCPServer & tcp_server_,
        bool ssl_enabled_,
        Int32 connection_id_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void run() final;

private:
    LoggerPtr log = getLogger("MongoHandler");

    IServer & server;
    TCPServer & tcp_server;
    std::unique_ptr<Session> session;
    bool ssl_enabled = false;
    Int32 connection_id = 0;
    Int32 secret_key = 0;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    std::shared_ptr<MongoProtocol::MessageTransport> message_transport;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    void changeIO(Poco::Net::StreamSocket & socket);
};

}
