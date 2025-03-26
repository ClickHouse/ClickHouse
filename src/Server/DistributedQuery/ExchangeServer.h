#pragma once

#include <Server/DistributedQuery/ExchangeConnections.h>
#include <Server/TCPServerConnectionFactory.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/Runnable.h>
#include <Common/Logger.h>

namespace DB
{

/// Accepts connections and reads first packet from them. This packet contains distributed query id and exchange stream id.
/// Then the connection is stored in a map and can be rertrieved by distributed query task to create ExchangeStreamingSource
class ExchangeServer : public Poco::Runnable
{
public:
    ExchangeServer(UInt16 port, ExchangeConnectionsPtr connections_);
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
