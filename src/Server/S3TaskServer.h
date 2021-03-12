#pragma once

#include <unordered_map>
#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_GRPC
#include <Poco/Net/SocketAddress.h>
#include "clickhouse_s3_task_server.grpc.pb.h"


#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#include <vector>
#include <string>
#include <optional>

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using clickhouse::s3_task_server::S3TaskServer;
using clickhouse::s3_task_server::S3TaskRequest;
using clickhouse::s3_task_server::S3TaskReply;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class S3Task
{   
public:
    S3Task() = delete;

    explicit S3Task(std::vector<std::string> && paths_)
        : paths(std::move(paths_))
    {}

    std::optional<std::string> getNext() {
        static size_t next = 0;
        if (next >= paths.size())
            return std::nullopt;
        const auto result = paths[next];
        ++next;
        return result;
    }
private:
    std::vector<std::string> paths;
};


// Logic and data behind the server's behavior.
class S3TaskServer final : public S3TaskServer::Service {
  Status GetNext(ServerContext* context, const S3TaskRequest* request, S3TaskReply* reply) override {
    std::string prefix("Hello");
    const auto query_id = request->query_id();
    auto it = handlers.find(query_id);
    if (it == handlers.end()) {
        reply->set_message("");
        reply->set_error(ErrorCodes::LOGICAL_ERROR);
        return Status::CANCELLED;
    }

    reply->set_error(0);
    reply->set_message(it->second.getNext());
    return Status::OK;
  }

  private:
    std::unordered_map<std::string, S3Task> handlers;
};


void RunServer() {
  std::string server_address("0.0.0.0:50051");
  static S3TaskServer service;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  server->Wait();
}

}


#endif
