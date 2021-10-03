#pragma once

#if !defined(ARCADIA_BUILD)
#   include <Common/config.h>
#   include "config_core.h"
#endif

#include <base/types.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

namespace Poco { class ThreadPool; }
namespace Poco::Net { class TCPServer; }

namespace DB
{

class AsynchronousMetrics;
class GRPCServer;
class IServer;
class InterfaceConfig;
using InterfaceConfigs = std::map<std::string, std::unique_ptr<InterfaceConfig>>;

/// Provides an unified interface to access a protocol implementing server
/// no matter what type it has (HTTPServer, TCPServer, MySQLServer, GRPCServer, ...)
/// and how many local endpoints are bound.
class ProtocolServerAdapter
{
public:
    ProtocolServerAdapter(const std::string & interface_name_);

    /// Adds a server to the list. All adds must be performed before any start or stop.
    void add(std::unique_ptr<Poco::Net::TCPServer> && server);
#if USE_GRPC
    void add(std::unique_ptr<GRPCServer> && server);
#endif

    /// Starts the servers. A new thread will be created that waits for and accepts incoming connections.
    void start();

    /// Stops the servers. No new connections will be accepted.
    void stop();

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

    /// Returns the number of current threads.
    size_t currentThreads() const;

    const std::string & getInterfaceName() const;

private:
    friend class ProtocolServers;

    class Impl
    {
    public:
        virtual ~Impl() {}
        virtual void start() = 0;
        virtual void stop() = 0;
        virtual size_t currentConnections() const = 0;
        virtual size_t currentThreads() const = 0;
    };

    class TCPServerAdapterImpl;
    class GRPCServerAdapterImpl;

    std::string interface_name;
    std::vector<std::unique_ptr<Impl>> servers;
};

namespace Util
{

std::vector<ProtocolServerAdapter> createServers(
    const std::vector<std::string> & protocols,
    const InterfaceConfigs & interfaces,
    IServer & server,
    Poco::ThreadPool & pool,
    AsynchronousMetrics * async_metrics
);

}

}
