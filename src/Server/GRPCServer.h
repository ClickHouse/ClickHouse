#pragma once

#include "config.h"

#if USE_GRPC

#include <Server/IProtocolServer.h>
#include <base/types.h>
#include <Poco/Net/SocketAddress.h>
#include <Common/Logger.h>
#include "clickhouse_grpc.grpc.pb.h"

namespace grpc
{
class Server;
class ServerCompletionQueue;
}

namespace DB
{
class IServer;

class GRPCServer : public IProtocolServer
{
public:
    GRPCServer(
        const std::string & listen_host_,
        const std::string & port_name_,
        const std::string & description_,
        IServer & iserver_,
        const Poco::Net::SocketAddress & address_to_listen_);

    ~GRPCServer() override;

    void start() override;

    void stop() override;

    bool isStopping() const override { return is_stopping; }

    UInt16 portNumber() const override { return address_to_listen.port(); }

    size_t currentConnections() const override;

    size_t currentThreads() const override { return currentConnections(); }

private:
    using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
    class Runner;

    IServer & iserver;
    const Poco::Net::SocketAddress address_to_listen;
    LoggerRawPtr log;
    GRPCService grpc_service;
    std::unique_ptr<grpc::Server> grpc_server;
    std::unique_ptr<grpc::ServerCompletionQueue> queue;
    std::unique_ptr<Runner> runner;
    bool is_stopping = false;
};
}
#endif
