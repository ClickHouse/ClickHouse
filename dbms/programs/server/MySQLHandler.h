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
    MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, RSA & public_key_, RSA & private_key_, bool ssl_enabled, size_t connection_id_);

    void run() final;

private:
    /// Enables SSL, if client requested.
    void finishHandshake(MySQLProtocol::HandshakeResponse &);

    void comQuery(ReadBuffer & payload);

    void comFieldList(ReadBuffer & payload);

    void comPing();

    void comInitDB(ReadBuffer & payload);

    void authenticate(const String & user_name, const String & auth_plugin_name, const String & auth_response);

    IServer & server;
    Poco::Logger * log;
    Context connection_context;

    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;

    size_t connection_id = 0;

    size_t server_capability_flags = 0;
    size_t client_capability_flags = 0;

    RSA & public_key;
    RSA & private_key;

    std::unique_ptr<MySQLProtocol::Authentication::IPlugin> auth_plugin;

    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    bool secure_connection = false;
};

}
