#pragma once

#include <optional>
#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Common/CurrentMetrics.h>
#include <Common/ProfileEvents.h>
#include "IO/ReadBufferFromString.h"
#include "IServer.h"

#include "base/types.h"
#include "config.h"

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
    /// statement_id -> statement
    using PreparedStatements = std::unordered_map<UInt32, String>;

public:
    MySQLHandler(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        bool ssl_enabled,
        uint32_t connection_id_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    ~MySQLHandler() override;

    void run() final;

protected:
    CurrentMetrics::Increment metric_increment{CurrentMetrics::MySQLConnection};

    /// Enables SSL, if client requested.
    void finishHandshake(MySQLProtocol::ConnectionPhase::HandshakeResponse &);

    void comQuery(ReadBuffer & payload, bool binary_protocol);

    void comFieldList(ReadBuffer & payload);

    void comPing();

    void comInitDB(ReadBuffer & payload);

    void authenticate(const String & user_name, const String & auth_plugin_name, const String & auth_response);

    void comStmtPrepare(ReadBuffer & payload);

    void comStmtExecute(ReadBuffer & payload);

    void comStmtClose(ReadBuffer & payload);

    /// Contains statement_id if the statement was emplaced successfully
    std::optional<UInt32> emplacePreparedStatement(String statement);
    /// Contains statement as a buffer if we could find previously stored statement using provided statement_id
    std::optional<ReadBufferFromString> getPreparedStatement(UInt32 statement_id);
    void erasePreparedStatement(UInt32 statement_id);

    virtual void authPluginSSL();
    virtual void finishHandshakeSSL(size_t packet_size, char * buf, size_t pos, std::function<void(size_t)> read_bytes, MySQLProtocol::ConnectionPhase::HandshakeResponse & packet);

    IServer & server;
    TCPServer & tcp_server;
    LoggerPtr log;
    uint32_t connection_id = 0;

    uint32_t server_capabilities = 0;
    uint32_t client_capabilities = 0;
    size_t max_packet_size = 0;
    uint8_t sequence_id = 0;

    MySQLProtocol::PacketEndpointPtr packet_endpoint;
    std::unique_ptr<Session> session;

    using QueryReplacementFn = std::function<String(const String & query)>;
    using QueriesReplacements = std::unordered_map<std::string, QueryReplacementFn>;
    QueriesReplacements queries_replacements;

    /// MySQL setting name --> ClickHouse setting name
    using SettingsReplacements = std::unordered_map<std::string, std::string>;
    SettingsReplacements settings_replacements;

    std::mutex prepared_statements_mutex;
    UInt32 current_prepared_statement_id TSA_GUARDED_BY(prepared_statements_mutex) = 0;
    PreparedStatements prepared_statements TSA_GUARDED_BY(prepared_statements_mutex);

    std::unique_ptr<MySQLProtocol::Authentication::IPlugin> auth_plugin;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    bool secure_connection = false;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;
};

#if USE_SSL
class MySQLHandlerSSL : public MySQLHandler
{
public:
    MySQLHandlerSSL(
        IServer & server_,
        TCPServer & tcp_server_,
        const Poco::Net::StreamSocket & socket_,
        bool ssl_enabled,
        uint32_t connection_id_,
        RSA & public_key_,
        RSA & private_key_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

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
