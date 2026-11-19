#pragma once

#if defined(OS_LINUX) || defined(OS_DARWIN)

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/DistributedQuery/ExchangeControlConnectionHandler.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Runnable.h>
#include <Common/Logger.h>
#include <Common/ThreadPool.h>

namespace DB
{

/// Accepts connections for streaming exchanges used by distributed queries.
//  Reads first packet from the connections that contains distributed query id and exchange stream id.
/// Then the connection is stored in a map and can be retrieved by distributed query task to create ExchangeStreamingSource
class ExchangeServer : public Poco::Runnable
{
public:
    /// Without `control_handler_`, a connection that opens with a control packet gets an `Error` and is closed.
    ExchangeServer(const String & listen_host, UInt16 port, UInt32 listen_backlog, ExchangeConnectionsPtr connections_,
        ExchangeConnectionAuthenticator authenticate_connection_ = {}, ExchangeControlConnectionHandlerPtr control_handler_ = nullptr);
    ~ExchangeServer() override;

    void start();
    void stop();
    /// The port the server listens on; tells the port chosen for a listen port of 0.
    UInt16 port() const { return server_socket.address().port(); }

    void run() override;

    /// Runs the SourceHello/SinkHello handshake on `socket` and registers the
    /// resulting connection in `connections` on success. When `authenticate`
    /// is set, the SourceHello auth token is checked before registration and a failure
    /// throws without registering. Throws on protocol mismatch or transport
    /// failure without registering. A connection that opens with a control packet goes to
    /// `control_handler`. Exposed for tests.
    static void handleConnection(Poco::Net::StreamSocket socket, ExchangeConnectionsPtr connections, LoggerPtr log, const ExchangeConnectionAuthenticator & authenticate,
        const ExchangeControlConnectionHandlerPtr & control_handler = nullptr);

private:
    ExchangeConnectionsPtr connections;
    ExchangeConnectionAuthenticator authenticate_connection;
    ExchangeControlConnectionHandlerPtr control_handler;
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
