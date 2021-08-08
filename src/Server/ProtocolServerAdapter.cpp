#include <Server/ProtocolServerAdapter.h>
#include <Server/ProtocolInterfaceConfig.h>
#include <Server/IServer.h>
#include <Interpreters/Context.h>
#include <Poco/Net/TCPServer.h>
#include <boost/algorithm/string.hpp>

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#include <set>

namespace DB
{

class ProtocolServerAdapter::TCPServerAdapterImpl : public Impl
{
public:
    explicit TCPServerAdapterImpl(std::unique_ptr<Poco::Net::TCPServer> tcp_server_) : tcp_server(std::move(tcp_server_)) {}
    ~TCPServerAdapterImpl() override = default;

    void start() override { tcp_server->start(); }
    void stop() override { tcp_server->stop(); }
    size_t currentConnections() const override { return tcp_server->currentConnections(); }
    size_t currentThreads() const override { return tcp_server->currentThreads(); }

private:
    std::unique_ptr<Poco::Net::TCPServer> tcp_server;
};

void ProtocolServerAdapter::add(std::unique_ptr<Poco::Net::TCPServer> && server)
{
    if (server)
        servers.emplace_back(std::make_unique<TCPServerAdapterImpl>(std::move(server)));
}

#if USE_GRPC
class ProtocolServerAdapter::GRPCServerAdapterImpl : public Impl
{
public:
    explicit GRPCServerAdapterImpl(std::unique_ptr<GRPCServer> grpc_server_) : grpc_server(std::move(grpc_server_)) {}
    ~GRPCServerAdapterImpl() override = default;

    void start() override { grpc_server->start(); }
    void stop() override { grpc_server->stop(); }
    size_t currentConnections() const override { return grpc_server->currentConnections(); }
    size_t currentThreads() const override { return grpc_server->currentThreads(); }

private:
    std::unique_ptr<GRPCServer> grpc_server;
};

void ProtocolServerAdapter::add(std::unique_ptr<GRPCServer> && server)
{
    if (server)
        servers.emplace_back(std::make_unique<GRPCServerAdapterImpl>(std::move(server)));
}
#endif

ProtocolServerAdapter::ProtocolServerAdapter(const std::string & interface_name_)
    : interface_name(interface_name_)
{
}

void ProtocolServerAdapter::start()
{
    for (auto & server : servers)
        server->start();
}

void ProtocolServerAdapter::stop()
{
    for (auto & server : servers)
        server->stop();
}

size_t ProtocolServerAdapter::currentConnections() const
{
    size_t res = 0;

    for (auto & server : servers)
        res += server->currentConnections();

    return res;
}

size_t ProtocolServerAdapter::currentThreads() const
{
    size_t res = 0;

    for (auto & server : servers)
        res += server->currentThreads();

    return res;
}

const std::string & ProtocolServerAdapter::getInterfaceName() const
{
    return interface_name;
}

namespace Util
{

std::vector<ProtocolServerAdapter> createServers(
    const std::vector<std::string> & protocols,
    const ProtocolInterfaceConfigs & interfaces,
    IServer & server,
    Poco::ThreadPool & pool,
    AsynchronousMetrics * async_metrics
)
{
    std::vector<ProtocolServerAdapter> servers;
    std::set<std::string> uniques;

    // Preserve protocols order.
    for (auto protocol : protocols)
    {
        // Skip duplicates.
        if (uniques.insert(boost::to_lower_copy(protocol)).second)
        {
            for (const auto & interface : interfaces)
            {
                // Choose only applicable interfaces.
                if (boost::iequals(protocol, interface.second->protocol))
                    servers.push_back(interface.second->createServerAdapter(server, pool, async_metrics));
            }
        }
    }

    return servers;
}

}

}
