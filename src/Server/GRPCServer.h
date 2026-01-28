#pragma once

#include "config.h"

#if USE_GRPC

#include "clickhouse_grpc.grpc.pb.h"
#include <Poco/Net/SocketAddress.h>
#include <base/types.h>
#include <Common/Logger.h>

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

    /// Returns the port this server is listening to.
    UInt16 portNumber() const { return address_to_listen.port(); }

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

    /// Returns the number of current threads.
    size_t currentThreads() const { return currentConnections(); }

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
};
}
#endif
