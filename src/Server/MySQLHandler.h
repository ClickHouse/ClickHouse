#pragma once

#include <Poco/Net/TCPServerConnection.h>
#include <common/getFQDNOrHostName.h>
#include <Common/CurrentMetrics.h>
#include <Core/MySQLProtocol.h>
#include "IServer.h"

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric MySQLConnection;
}

namespace DB
{
/// Handler for MySQL wire protocol connections. Allows to connect to ClickHouse using MySQL client.
class MySQLHandler : public Poco::Net::TCPServerConnection
{
public:
    MySQLHandler(IServer & server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, size_t connection_id_);

    void run() final;

private:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::MySQLConnection};

    /// Enables SSL, if client requested.
    void finishHandshake(MySQLProtocol::HandshakeResponse &);

    void comQuery(ReadBuffer & payload);

    void comFieldList(ReadBuffer & payload);

    void comPing();

    void comInitDB(ReadBuffer & payload);

    void authenticate(const String & user_name, const String & auth_plugin_name, const String & auth_response);

    virtual void authPluginSSL();
    virtual void finishHandshakeSSL(size_t packet_size, char * buf, size_t pos, std::function<void(size_t)> read_bytes, MySQLProtocol::HandshakeResponse & packet);

    IServer & server;

protected:
    Poco::Logger * log;

    Context connection_context;

    std::shared_ptr<MySQLProtocol::PacketSender> packet_sender;

private:
    size_t connection_id = 0;

    size_t server_capability_flags = 0;
    size_t client_capability_flags = 0;

protected:
    std::unique_ptr<MySQLProtocol::Authentication::IPlugin> auth_plugin;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;

    bool secure_connection = false;

private:
    using ReplacementFn = std::function<String(const String & query)>;
    using Replacements = std::unordered_map<std::string, ReplacementFn>;
    Replacements replacements;
};

#if USE_SSL
class MySQLHandlerSSL : public MySQLHandler
{
public:
    MySQLHandlerSSL(IServer & server_, const Poco::Net::StreamSocket & socket_, bool ssl_enabled, size_t connection_id_, RSA & public_key_, RSA & private_key_);

private:
    void authPluginSSL() override;
    void finishHandshakeSSL(size_t packet_size, char * buf, size_t pos, std::function<void(size_t)> read_bytes, MySQLProtocol::HandshakeResponse & packet) override;

    RSA & public_key;
    RSA & private_key;
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
};
#endif

}
