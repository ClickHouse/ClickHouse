#pragma once

#include <Common/config.h>

#include <base/types.h>
#include <Poco/Net/SocketAddress.h>


namespace DB
{
class IServer;

class AsyncTCPServer
{
public:
    AsyncTCPServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_);
    ~AsyncTCPServer();

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start();

    /// Stops the server. No new connections will be accepted.
    void stop();

    /// Returns the port this server is listening to.
    UInt16 portNumber() const { return address_to_listen.port(); }

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

    /// Returns the number of current threads.
    size_t currentThreads() const { return currentConnections(); }

private:
    IServer & iserver;
    const Poco::Net::SocketAddress address_to_listen;
};

}
