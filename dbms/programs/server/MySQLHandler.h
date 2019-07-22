#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <Poco/Net/SecureStreamSocket.h>
#include <Common/getFQDNOrHostName.h>
#include <Core/MySQLProtocol.h>
#include "IServer.h"


namespace DB
{

/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection
{
public:
    MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, RSA & public_key, RSA & private_key, bool ssl_enabled, size_t connection_id);

    void run() final;

private:
    /// Enables SSL, if client requested.
    MySQLProtocol::HandshakeResponse finishHandshake();

    void comQuery(const String & payload);

    void comFieldList(const String & payload);

    void comPing();

    void comInitDB(const String & payload);

    static String generateScramble();

    void authenticate(const MySQLProtocol::HandshakeResponse &, const String & scramble);

    IServer & server;
    Poco::Logger * log;
    Context connection_context;

    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;

    size_t connection_id = 0;

    size_t server_capability_flags = 0;
    size_t client_capability_flags = 0;

    RSA & public_key;
    RSA & private_key;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    bool secure_connection = false;
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
};

}
