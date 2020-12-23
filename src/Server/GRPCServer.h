#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_GRPC
#include <Poco/Net/SocketAddress.h>
#include "clickhouse_grpc.grpc.pb.h"

namespace Poco { class Logger; }

namespace grpc
{
class Server;
class ServerCompletionQueue;
}

namespace DB
{
class IServer;

class GRPCServer
{
public:
    GRPCServer(IServer & iserver_, const Poco::Net::SocketAddress & address_to_listen_);
    ~GRPCServer();

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start();

    /// Stops the server. No new connections will be accepted.
    void stop();

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

    /// Returns the number of current threads.
    size_t currentThreads() const { return currentConnections(); }

private:
    using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
    class Runner;

    IServer & iserver;
    const Poco::Net::SocketAddress address_to_listen;
    Poco::Logger * log;
    GRPCService grpc_service;
    std::unique_ptr<grpc::Server> grpc_server;
    std::unique_ptr<grpc::ServerCompletionQueue> queue;
    std::unique_ptr<Runner> runner;
};
}
#endif
