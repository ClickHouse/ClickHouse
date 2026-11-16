#include <Server/ProtocolServerAdapter.h>
#include <Server/TCPServer.h>

#if USE_GRPC
#include <Server/IGRPCServer.h>
#endif


namespace DB
{
class ProtocolServerAdapter::TCPServerAdapterImpl : public Impl
{
public:
    explicit TCPServerAdapterImpl(std::unique_ptr<TCPServer> tcp_server_) : tcp_server(std::move(tcp_server_)) {}
    ~TCPServerAdapterImpl() override = default;

    void start() override { tcp_server->start(); }
    void stop() override { tcp_server->stop(); }
    [[nodiscard]] bool isStopping() const override { return !tcp_server->isOpen(); }
    [[nodiscard]] bool bindsOnStart() const override { return false; }
    [[nodiscard]] UInt16 portNumber() const override { return tcp_server->portNumber(); }
    [[nodiscard]] size_t currentConnections() const override { return tcp_server->currentConnections(); }
    [[nodiscard]] size_t currentThreads() const override { return tcp_server->currentThreads(); }
    [[nodiscard]] size_t refusedConnections() const override { return tcp_server->refusedConnections(); }

private:
    std::unique_ptr<TCPServer> tcp_server;
};

ProtocolServerAdapter::ProtocolServerAdapter(
    const std::string & listen_host_,
    const char * port_name_,
    ServerType::Type protocol_type_,
    const std::string & description_,
    std::unique_ptr<TCPServer> tcp_server_,
    bool supports_runtime_reconfiguration_)
    : listen_host(listen_host_)
    , port_name(port_name_)
    , protocol_type(protocol_type_)
    , description(description_)
    , impl(std::make_unique<TCPServerAdapterImpl>(std::move(tcp_server_)))
    , supports_runtime_reconfiguration(supports_runtime_reconfiguration_)
{
}

#if USE_GRPC
class ProtocolServerAdapter::GRPCServerAdapterImpl : public Impl
{
public:
    explicit GRPCServerAdapterImpl(std::unique_ptr<IGRPCServer> grpc_server_) : grpc_server(std::move(grpc_server_)) {}
    ~GRPCServerAdapterImpl() override = default;

    void start() override { grpc_server->start(); }
    void stop() override
    {
        is_stopping = true;
        grpc_server->stop();
    }
    [[nodiscard]] bool isStopping() const override { return is_stopping; }
    [[nodiscard]] bool bindsOnStart() const override { return true; }
    [[nodiscard]] UInt16 portNumber() const override { return grpc_server->portNumber(); }
    [[nodiscard]] size_t currentConnections() const override { return grpc_server->currentConnections(); }
    [[nodiscard]] size_t currentThreads() const override { return grpc_server->currentThreads(); }
    [[nodiscard]] size_t refusedConnections() const override { return 0; }

private:
    std::unique_ptr<IGRPCServer> grpc_server;
    bool is_stopping = false;
};

ProtocolServerAdapter::ProtocolServerAdapter(
    const std::string & listen_host_,
    const char * port_name_,
    ServerType::Type protocol_type_,
    const std::string & description_,
    std::unique_ptr<IGRPCServer> grpc_server_,
    bool supports_runtime_reconfiguration_)
    : listen_host(listen_host_)
    , port_name(port_name_)
    , protocol_type(protocol_type_)
    , description(description_)
    , impl(std::make_unique<GRPCServerAdapterImpl>(std::move(grpc_server_)))
    , supports_runtime_reconfiguration(supports_runtime_reconfiguration_)
{
}
#endif
}
