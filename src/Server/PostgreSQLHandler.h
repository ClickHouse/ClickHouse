#pragma once

#include <Common/CurrentMetrics.h>
#include "config.h"
#include <Core/PostgreSQLProtocol.h>
#include <Poco/Net/TCPServerConnection.h>
#include <Server/IServer.h>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric PostgreSQLConnection;
}

namespace DB
{
class ReadBufferFromPocoSocket;
class Session;
class TCPServer;

/** PostgreSQL wire protocol implementation.
 * For more info see https://www.postgresql.org/docs/current/protocol.html
 */
class PostgreSQLHandler : public Poco::Net::TCPServerConnection
{
public:
    PostgreSQLHandler(
        const Poco::Net::StreamSocket & socket_,
#if USE_SSL
        const String & prefix_,
#endif
        IServer & server_,
        TCPServer & tcp_server_,
        bool ssl_enabled_,
        bool secure_required_,
        Int32 connection_id_,
        std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_,
        const ProfileEvents::Event & read_event_ = ProfileEvents::end(),
        const ProfileEvents::Event & write_event_ = ProfileEvents::end());

    void run() final;

private:
    LoggerPtr log = getLogger("PostgreSQLHandler");

#if USE_SSL
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;

    Poco::Net::Context::Params params [[maybe_unused]];
    Poco::Net::Context::Usage usage [[maybe_unused]];
    int disabled_protocols = 0;
    bool extended_verification = false;
    bool prefer_server_ciphers = false;
    const Poco::Util::LayeredConfiguration & config [[maybe_unused]];
    String prefix [[maybe_unused]];
#endif

    IServer & server;
    TCPServer & tcp_server;
    std::unique_ptr<Session> session;
    bool ssl_enabled = false;
    bool secure_required = false;
    Int32 connection_id = 0;
    Int32 secret_key = 0;

    bool is_query_in_progress = false;

    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBuffer> out;
    std::shared_ptr<PostgreSQLProtocol::Messaging::MessageTransport> message_transport;

    ProfileEvents::Event read_event;
    ProfileEvents::Event write_event;

    PostgreSQLProtocol::PGAuthentication::AuthenticationManager authentication_manager;
    PostgreSQLProtocol::PostgresPreparedStatements::PreparedStatemetsManager prepared_statements_manager;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::PostgreSQLConnection};

    void changeIO(Poco::Net::StreamSocket & socket);

    bool startup();

    void establishSecureConnection(Int32 & payload_size, Int32 & info);

    void makeSecureConnectionSSL();

    void sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message);

    void cancelRequest();

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> receiveStartupMessage(int payload_size);

    void processQuery();

    bool processPrepareStatement(const String & query);
    bool processExecute(const String & query, ContextMutablePtr query_context);
    bool processDeallocate(const String & query);
    bool processCopyQuery(const String & query);

    void processParseQuery();
    void processDescribeQuery();
    void processBindQuery();
    void processExecuteQuery();
    void processCloseQuery();
    void processSyncQuery();

    std::function<void(const Progress&)> createProgressCallback(
        ContextMutablePtr query_context,
        std::atomic<UInt64>& result_rows,
        std::atomic<UInt64>& written_rows);

    UInt64 executeQueryWithTracking(
        String && sql_query,
        ContextMutablePtr query_context,
        PostgreSQLProtocol::Messaging::CommandComplete::Command command);

    static bool isEmptyQuery(const String & query);
    static Int32 parseNumberColumns(const std::vector<char> & output);
};

}
