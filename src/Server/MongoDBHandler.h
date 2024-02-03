#pragma once

#include "Common/ProfileEvents.h"
#include <Common/CurrentMetrics.h>
#include "IO/WriteBuffer.h"
#include "config.h"
// #include <Core/PostgreSQLProtocol.h>
#include <Poco/Net/TCPServerConnection.h>
#include "IServer.h"

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric MongoDBConnection;
}

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** MongoDB wire protocol implementation.
 * For more info see https://www.postgresql.org/docs/current/protocol.html // FIXME
 */
class MongoDBHandler : public Poco::Net::TCPServerConnection
{
public:
    MongoDBHandler(
        const Poco::Net::StreamSocket & socket_,
        IServer & server_,
        TCPServer & tcp_server_,
        Int32 connection_id_);

    void run() final;

    void handleInput();

private:
    constexpr static const auto handler_name = "MongoDBHandler";
    LoggerPtr log = getLogger(handler_name);

    IServer & server;
    TCPServer & tcp_server;
    std::unique_ptr<Session> session;
    Int32 connection_id = 0;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    // std::shared_ptr<PostgreSQLProtocol::Messaging::MessageTransport> message_transport;

#if USE_SSL
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
#endif

};

}
