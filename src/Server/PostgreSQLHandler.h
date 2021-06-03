#pragma once

#include <Common/CurrentMetrics.h>
#include <Core/PostgreSQLProtocol.h>
#include <Poco/Net/TCPServerConnection.h>
#include <common/logger_useful.h>
#include "IServer.h"

#if USE_SSL
#   include <Poco/Net/SecureStreamSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric PostgreSQLConnection;
}

namespace DB
{

/** PostgreSQL wire protocol implementation.
 * For more info see https://www.postgresql.org/docs/current/protocol.html
 */
class PostgreSQLHandler : public Poco::Net::TCPServerConnection
{
public:
    PostgreSQLHandler(
        const Poco::Net::StreamSocket & socket_,
        IServer & server_,
        bool ssl_enabled_,
        Int32 connection_id_,
        std::vector<std::shared_ptr<PostgreSQLProtocol::PGAuthentication::AuthenticationMethod>> & auth_methods_);

    void run() final;

private:
    Poco::Logger * log = &Poco::Logger::get("PostgreSQLHandler");

    IServer & server;
    Context connection_context;
    bool ssl_enabled;
    Int32 connection_id;
    Int32 secret_key;

    std::shared_ptr<ReadBuffer> in;
    std::shared_ptr<WriteBuffer> out;
    std::shared_ptr<PostgreSQLProtocol::Messaging::MessageTransport> message_transport;

#if USE_SSL
    std::shared_ptr<Poco::Net::SecureStreamSocket> ss;
#endif

    PostgreSQLProtocol::PGAuthentication::AuthenticationManager authentication_manager;

    CurrentMetrics::Increment metric_increment{CurrentMetrics::PostgreSQLConnection};

    void changeIO(Poco::Net::StreamSocket & socket);

    bool startup();

    void establishSecureConnection(Int32 & payload_size, Int32 & info);

    void makeSecureConnectionSSL();

    void sendParameterStatusData(PostgreSQLProtocol::Messaging::StartupMessage & start_up_message);

    void cancelRequest();

    std::unique_ptr<PostgreSQLProtocol::Messaging::StartupMessage> receiveStartupMessage(int payload_size);

    void processQuery();

    static bool isEmptyQuery(const String & query);
};

}
