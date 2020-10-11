#pragma once

#include <atomic>
#include <iostream>
#include <string>
#include <memory>
#include <mutex>
#include <Common/Stopwatch.h>
#include <DataStreams/BlockIO.h>
#include <IO/Progress.h>
#include <IO/WriteBufferFromString.h>
#include "IServer.h"
#include <Poco/RunnableAdapter.h>
#include <Processors/Executors/PullingPipelineExecutor.h>

#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include "GrpcConnection.grpc.pb.h"

#include "WriteBufferFromGRPC.h"

using GRPCConnection::QueryRequest;
using GRPCConnection::QueryResponse;
using GRPCConnection::GRPC;

namespace DB
{

class CommonCallData
{
     public:
     GRPC::AsyncService* service;
     grpc::ServerCompletionQueue* notification_cq;
     grpc::ServerCompletionQueue* new_call_cq;
     grpc::ServerContext gRPCcontext;
     IServer* iServer;
     bool with_stacktrace = false;
     Poco::Logger * log;
     std::unique_ptr<CommonCallData> next_client;

     public:
          explicit CommonCallData(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* notification_cq_, grpc::ServerCompletionQueue* new_call_cq_, IServer* iServer_, Poco::Logger * log_)
          : service(Service_), notification_cq(notification_cq_), new_call_cq(new_call_cq_), iServer(iServer_), log(log_)
          {}
          virtual ~CommonCallData()
          {}
          virtual void respond() = 0;
};

class CallDataQuery : public CommonCallData
{
     public:
          CallDataQuery(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* notification_cq_, grpc::ServerCompletionQueue* new_call_cq_, IServer* server_, Poco::Logger * log_)
          : CommonCallData(Service_, notification_cq_, new_call_cq_, server_, log_), responder(&gRPCcontext), context(iServer->context())
          {
               detailsStatus = SEND_TOTALS;
               status = START_QUERY;
               out = std::make_shared<WriteBufferFromGRPC>(&responder, (void*)this, nullptr);
               service->RequestQuery(&gRPCcontext, &responder, new_call_cq, notification_cq, this);
          }
          void ParseQuery();
          void ParseData();
          void ReadData();
          void ExecuteQuery();
          void ProgressQuery();
          void FinishQuery();

          enum DetailsStatus
          {
               SEND_TOTALS,
               SEND_EXTREMES,
               SEND_PROFILEINFO,
               FINISH
          };
          void SendDetails();

          bool sendData(const Block & block);
          bool sendProgress();
          bool sendTotals(const Block & totals);
          bool sendExtremes(const Block & block);

          enum Status
          {
               START_QUERY,
               PARSE_QUERY,
               READ_DATA,
               PROGRESS,
               FINISH_QUERY
          };
          virtual void respond() override;
          virtual ~CallDataQuery() override
          {
               query_watch.stop();
               progress_watch.stop();
               query_context.reset();
               query_scope.reset();
          }

     private:
          QueryRequest request;
          QueryResponse response;
          grpc::ServerAsyncReaderWriter<QueryResponse, QueryRequest> responder;

          Stopwatch progress_watch;
          Stopwatch query_watch;
          Progress progress;

          DetailsStatus detailsStatus;
          Status status;

          BlockIO io;
          Context context;
          std::shared_ptr<PullingPipelineExecutor> executor;
          std::optional<Context> query_context;

          std::shared_ptr<WriteBufferFromGRPC> out;
          String format_output;
          String format_input;
          uint64_t interactive_delay;
          std::optional<CurrentThread::QueryScope> query_scope;


};

class GRPCServer final : public Poco::Runnable
{
     public:
     GRPCServer(const GRPCServer &handler) = delete;
     GRPCServer(GRPCServer &&handler) = delete;
     GRPCServer(std::string server_address_, IServer & server_) : iServer(server_), log(&Poco::Logger::get("GRPCHandler"))
     {
          grpc::ServerBuilder builder;
          builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
          //keepalive pings default values
          builder.RegisterService(&service);
          builder.SetMaxReceiveMessageSize(INT_MAX);
          notification_cq = builder.AddCompletionQueue();
          new_call_cq = builder.AddCompletionQueue();
          server = builder.BuildAndStart();
     }
     void stop()
     {
          server->Shutdown();
          notification_cq->Shutdown();
          new_call_cq->Shutdown();
     }
     virtual void run() override
     {
          HandleRpcs();
     }
     void HandleRpcs()
     {
          new CallDataQuery(&service, notification_cq.get(), new_call_cq.get(), &iServer, log);

          // rpc event "read done / write done / close(already connected)" call-back by this completion queue
          auto handle_calls_completion = [&]()
          {
               void* tag;
               bool ok;
               while (true)
               {
                    GPR_ASSERT(new_call_cq->Next(&tag, &ok));
                    if (!ok)
                    {
                         LOG_WARNING(log, "Client has gone away.");
                         delete static_cast<CallDataQuery*>(tag);
                         continue;
                    }
                    auto thread = ThreadFromGlobalPool{&CallDataQuery::respond, static_cast<CallDataQuery*>(tag)};
                    thread.detach();
               }
          };
          // rpc event "new connection / close(waiting for connect)" call-back by this completion queue
          auto handle_requests_completion = [&]
          {
               void* tag;
               bool ok;
               while (true)
               {
                    GPR_ASSERT(notification_cq->Next(&tag, &ok));
                    if (!ok)
                    {
                         LOG_WARNING(log, "Client has gone away.");
                         delete static_cast<CallDataQuery*>(tag);
                         continue;
                    }
                    auto thread = ThreadFromGlobalPool{&CallDataQuery::respond, static_cast<CallDataQuery*>(tag)};
                    thread.detach();
               }
          };

          auto notification_cq_thread = ThreadFromGlobalPool{handle_requests_completion};
          auto new_call_cq_thread = ThreadFromGlobalPool{handle_calls_completion};
          notification_cq_thread.detach();
          new_call_cq_thread.detach();
     }

     private:
          IServer & iServer;
          Poco::Logger * log;
          std::unique_ptr<grpc::ServerCompletionQueue> notification_cq;
          std::unique_ptr<grpc::ServerCompletionQueue> new_call_cq;
          GRPC::AsyncService service;
          std::unique_ptr<grpc::Server> server;
          std::string server_address;
};

 }
