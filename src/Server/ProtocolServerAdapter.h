#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#include <memory>

namespace Poco::Net { class TCPServer; }

namespace DB
{
class GRPCServer;

/// Provides an unified interface to access a protocol implementing server
/// no matter what type it has (HTTPServer, TCPServer, MySQLServer, GRPCServer, ...).
class ProtocolServerAdapter
{
public:
    ProtocolServerAdapter() {}
    ProtocolServerAdapter(ProtocolServerAdapter && src) = default;
    ProtocolServerAdapter & operator =(ProtocolServerAdapter && src) = default;
    ~ProtocolServerAdapter() {}

    ProtocolServerAdapter(std::unique_ptr<Poco::Net::TCPServer> tcp_server_);

#if USE_GRPC
    ProtocolServerAdapter(std::unique_ptr<GRPCServer> grpc_server_);
#endif

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start() { impl->start(); }

    /// Stops the server. No new connections will be accepted.
    void stop() { impl->stop(); }

    /// Returns the number of currently handled connections.
    size_t currentConnections() const { return impl->currentConnections(); }

private:
    class Impl
    {
    public:
        virtual ~Impl() {}
        virtual void start() = 0;
        virtual void stop() = 0;
        virtual size_t currentConnections() const = 0;
    };
    class TCPServerAdapterImpl;
    class GRPCServerAdapterImpl;

    std::unique_ptr<Impl> impl;
};

}
