#pragma once

#ifdef OS_LINUX
#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Runnable.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

#include <functional>

namespace DB
{

/// Authenticates an incoming exchange connection from the JWT presented in
/// SourceHello (empty when the peer sent none). Throws to reject the connection
/// before it is registered. Empty (default, and the case when no authenticator is
/// configured) means authentication is not enforced and the token is ignored; a
/// caller can inject a closure that verifies the token.
using ExchangeConnectionAuthenticator = std::function<void(const String & jwt_token)>;

/// Accepts connections for streaming exchanges used by distributed queries.
//  Reads first packet from the connections that contains distributed query id and exchange stream id.
/// Then the connection is stored in a map and can be retrieved by distributed query task to create ExchangeStreamingSource
class ExchangeServer : public Poco::Runnable
{
public:
    ExchangeServer(const String & listen_host, UInt16 port, ExchangeConnectionsPtr connections_, ExchangeConnectionAuthenticator authenticate_connection_ = {});
    ~ExchangeServer() override;

    void start();
    void stop();

    void run() override;

    /// Runs the SourceHello/SinkHello handshake on `socket` and registers the
    /// resulting connection in `connections` on success. When `authenticate`
    /// is set, the SourceHello JWT is checked before registration and a failure
    /// throws without registering. Throws on protocol mismatch or transport
    /// failure without registering. Exposed for tests.
    static void handleConnection(Poco::Net::StreamSocket socket, ExchangeConnectionsPtr connections, LoggerPtr log, const ExchangeConnectionAuthenticator & authenticate);

private:
    ExchangeConnectionsPtr connections;
    ExchangeConnectionAuthenticator authenticate_connection;
    Poco::Net::ServerSocket server_socket;
    Poco::Thread accept_thread;
    /// Handshakes run on this pool so a slow peer cannot stall the single accept thread and block
    /// subsequent connections.
    ThreadPool handshake_pool;
    std::atomic<bool> stopped {false};
    LoggerPtr log;
};

}
#endif
