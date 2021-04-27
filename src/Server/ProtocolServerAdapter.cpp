#include <Server/ProtocolServerAdapter.h>
#include <Poco/Net/TCPServer.h>

#if USE_GRPC
#include <Server/GRPCServer.h>
#endif


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

ProtocolServerAdapter::ProtocolServerAdapter(const char * port_name_, std::unique_ptr<Poco::Net::TCPServer> tcp_server_)
    : port_name(port_name_), impl(std::make_unique<TCPServerAdapterImpl>(std::move(tcp_server_)))
{
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

ProtocolServerAdapter::ProtocolServerAdapter(const char * port_name_, std::unique_ptr<GRPCServer> grpc_server_)
    : port_name(port_name_), impl(std::make_unique<GRPCServerAdapterImpl>(std::move(grpc_server_)))
{
}
#endif
}
