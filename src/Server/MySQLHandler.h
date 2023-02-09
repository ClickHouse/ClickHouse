#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <base/getFQDNOrHostName.h>
#include <Common/CurrentMetrics.h>
#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include "IServer.h"

#include <Common/config.h>

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif

#include <memory>

namespace CurrentMetrics
{
    extern const Metric MySQLConnection;
}

namespace DB
{
class ReadBufferFromPocoSocket;
class TCPServer;

/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection
{
public:
    MySQLHandler(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, size_t connection_id_);

    void run() final;

protected:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::MySQLConnection};

    /// Enables SSL, if client requested.
    void finishHandshake(MySQLProtocol::ConnectionPhase::HandshakeResponse &);

    void comQuery(ReadBuffer & payload);

    void comFieldList(ReadBuffer & payload);

    void comPing();

    void comInitDB(ReadBuffer & payload);

    void authenticate(const String & user_name, const String & auth_plugin_name, const String & auth_response);

    virtual void authPluginSSL();
    virtual void finishHandshakeSSL(size_t packet_size, char * buf, size_t pos, std::function<void(size_t)> read_bytes, MySQLProtocol::ConnectionPhase::HandshakeResponse & packet);

    IServer & server;
    TCPServer & tcp_server;
    Poco::Logger * log;
    UInt64 connection_id = 0;

    uint32_t server_capabilities = 0;
    uint32_t client_capabilities = 0;
    size_t max_packet_size = 0;
    uint8_t sequence_id = 0;

    MySQLProtocol::PacketEndpointPtr packet_endpoint;
    std::unique_ptr<Session> session;

    using ReplacementFn = std::function<String(const String & query)>;
    using Replacements = std::unordered_map<std::string, ReplacementFn>;
    Replacements replacements;

    std::unique_ptr<MySQLProtocol::Authentication::IPlugin> auth_plugin;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    bool secure_connection = false;
};

#if USE_SSL
class MySQLHandlerSSL : public MySQLHandler
{
public:
    MySQLHandlerSSL(IServer & server_, TCPServer & tcp_server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, size_t connection_id_, RSA & public_key_, RSA & private_key_);

private:
    void authPluginSSL() override;

    void finishHandshakeSSL(
        size_t packet_size, char * buf, size_t pos,
        std::function<void(size_t)> read_bytes, MySQLProtocol::ConnectionPhase::HandshakeResponse & packet) override;

    RSA & public_key;
    RSA & private_key;
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
};
#endif

}
