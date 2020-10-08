#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_GRPC
#include <Poco/Runnable.h>
#include <Poco/Net/SocketAddress.h>
#include "clickhouse_grpc.grpc.pb.h"

namespace Poco
{
    class Logger;
    class ThreadPool;
}

namespace grpc
{
class Server;
class ServerCompletionQueue;
}

namespace DB
{
class IServer;

class GRPCServer final : public Poco::Runnable
{
public:
    GRPCServer(IServer & server_, Poco::ThreadPool & thread_pool_, const Poco::Net::SocketAddress & address_to_listen_);
    ~GRPCServer() override;

    /// Starts the server. A new thread will be created that waits for and accepts incoming connections.
    void start();

    /// Stops the server. No new connections will be accepted.
    void stop();

    /// Returns the number of currently handled connections.
    size_t currentConnections() const;

private:
    virtual void run() override;
    void HandleRpcs();

    using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
    IServer & iserver;
    Poco::ThreadPool & thread_pool;
    Poco::Net::SocketAddress address_to_listen;
    Poco::Logger * log;
    std::unique_ptr<grpc::ServerCompletionQueue> notification_cq;
    std::unique_ptr<grpc::ServerCompletionQueue> new_call_cq;
    GRPCService grpc_service;
    std::unique_ptr<grpc::Server> grpc_server;
};
}
#endif
