#pragma once

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Runnable.h>
#include <Common/Logger.h>

namespace DB
{

/// Accepts connections for streaming exchanges used by distributed queries.
//  Reads first packet from the connections that contains distributed query id and exchange stream id.
/// Then the connection is stored in a map and can be retrieved by distributed query task to create ExchangeStreamingSource
class ExchangeServer : public Poco::Runnable
{
public:
    ExchangeServer(const String & listen_host, UInt16 port, ExchangeConnectionsPtr connections_);
    ~ExchangeServer() override;

    void start();
    void stop();

    void run() override;

private:
    void addConnection(Poco::Net::StreamSocket socket);

    ExchangeConnectionsPtr connections;
    Poco::Net::ServerSocket server_socket;
    Poco::Thread accept_thread;
    std::atomic<bool> stopped {false};
    LoggerPtr log;
};

}
