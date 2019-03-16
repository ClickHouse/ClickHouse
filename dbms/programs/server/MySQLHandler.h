#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Common/getFQDNOrHostName.h>
#include "IServer.h"


namespace DB
{

/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection {
public:
    MySQLHandler(IServer &server_, const Poco::Net::StreamSocket &socket_)
        : Poco::Net::TCPServerConnection(socket_)
        , server(server_)
        , log(&Poco::Logger::get("MySQLHandler"))
        , connection_context(server.context())
        , connection_id(last_connection_id++)
    {
    }

    void run() final;

    /** Reads one packet, incrementing sequence-id, and returns its payload.
      *  Currently, whole payload is loaded into memory.
      */
    String readPayload();

    /// Writes packet payload, incrementing sequence-id.
    void writePayload(std::string_view payload);

    void comQuery(const String & payload);

    void comFieldList(const String & payload);

    void comPing();

    void comInitDB(const String & payload);
private:
    IServer & server;
    Poco::Logger * log;
    Context connection_context;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    /// Packet sequence id
    unsigned char sequence_id = 0;

    uint32_t connection_id = 0;

    uint32_t capabilities;

    static uint32_t last_connection_id;
};

}
