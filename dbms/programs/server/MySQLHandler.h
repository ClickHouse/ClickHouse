#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Common/getFQDNOrHostName.h>
#include <Core/MySQLProtocol.h>
#include "IServer.h"


namespace DB
{

/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection
{
public:
    MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_)
        : Poco::Net::TCPServerConnection(socket_), server(server_), log(&Poco::Logger::get("MySQLHandler")),
          connection_context(server.context()), connection_id(last_connection_id++)
    {
    }

    void run() final;

    void comQuery(String payload);

    void comFieldList(String payload);

    void comPing();

    void comInitDB(String payload);

private:
    IServer & server;
    Poco::Logger * log;
    Context connection_context;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    MySQLProtocol::PacketSender packet_sender;

    uint32_t connection_id = 0;

    uint32_t capabilities;

    static uint32_t last_connection_id;
};

}
