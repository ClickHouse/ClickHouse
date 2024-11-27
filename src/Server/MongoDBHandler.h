#pragma once

#include <Core/MongoDB/Messages/MessageWriter.h>
#include <Core/MongoDB/Messages/OpMsgMessage.h>
#include <Poco/Net/TCPServerConnection.h>
#include "Common/ProfileEvents.h"
#include <Common/CurrentMetrics.h>
#include "IO/WriteBuffer.h"
#include "IServer.h"
#include "config.h"

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** MongoDB wire protocol implementation.
 * For more info see https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol
 */
class MongoDBHandler : public Poco::Net::TCPServerConnection
{
public:
    MongoDBHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_, Int32 connection_id_);

    void run() final;

    void handleInput();

    void handleQuery(MongoDB::OpMsgMessage::Ptr message, DB::MongoDB::MessageWriter writer);

private:
    constexpr static const auto handler_name = "MongoDBHandler";
    LoggerPtr log = getLogger(handler_name);

    IServer & server;
    TCPServer & tcp_server;
    std::unique_ptr<Session> session;
    Int32 connection_id = 0;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
};

}
