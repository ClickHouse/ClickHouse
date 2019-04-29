#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Common/getFQDNOrHostName.h>
#include <Core/MySQLProtocol.h>
#include <openssl/evp.h>
#include "IServer.h"


namespace DB
{

/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection
{
public:
    MySQLHandler(
        IServer & server_,
        const Poco::Net::StreamSocket & socket_,
        RSA * public_key,
        RSA * private_key)
            : Poco::Net::TCPServerConnection(socket_)
            , server(server_)
            , log(&Poco::Logger::get("MySQLHandler"))
            , connection_context(server.context())
            , connection_id(last_connection_id++)
            , public_key(public_key)
            , private_key(private_key)
    {
    }

    void run() final;

private:
    /// Enables SSL, if client requested.
    MySQLProtocol::HandshakeResponse finishHandshake();

    void comQuery(String payload);

    void comFieldList(String payload);

    void comPing();

    void comInitDB(String payload);

    static String generateScramble();

    void authenticate(const MySQLProtocol::HandshakeResponse &, const String & scramble);

    IServer & server;
    Poco::Logger * log;
    Context connection_context;

    MySQLProtocol::PacketSender packet_sender;

    uint32_t connection_id = 0;

    uint32_t capabilities;

    static uint32_t last_connection_id;

    RSA * public_key, * private_key;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    bool secure_connection = false;
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
};

}
