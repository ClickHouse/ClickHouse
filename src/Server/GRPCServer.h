#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_GRPC
#include <Poco/Runnable.h>
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

class GRPCServer final : public Poco::Runnable
{
public:
    GRPCServer(const GRPCServer & handler) = delete;
    GRPCServer(GRPCServer && handler) = delete;
    GRPCServer(std::string address_to_listen_, IServer & server_);
    ~GRPCServer() override;

    void stop();
    virtual void run() override;
    void HandleRpcs();

private:
    using GRPCService = clickhouse::grpc::ClickHouse::AsyncService;
    IServer & iserver;
    Poco::Logger * log;
    std::unique_ptr<grpc::ServerCompletionQueue> notification_cq;
    std::unique_ptr<grpc::ServerCompletionQueue> new_call_cq;
    GRPCService grpc_service;
    std::unique_ptr<grpc::Server> grpc_server;
    std::string address_to_listen;
};
}
#endif
