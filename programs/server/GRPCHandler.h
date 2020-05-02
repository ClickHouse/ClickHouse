#pragma once

#include <iostream>
#include <memory>
#include <atomic>
#include <string>
#include <mutex>
#include "IServer.h"
#include <Poco/RunnableAdapter.h>
#include <IO/Progress.h>
#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromString.h>
#include <DataStreams/BlockIO.h>

#include <grpc++/server.h>
#include <grpc++/server_builder.h>
#include <grpc++/server_context.h>
#include "GrpcConnection.grpc.pb.h"
#include <Processors/Formats/LazyOutputFormat.h>
#include "WriteBufferFromGRPC.h"

using GRPCConnection::QueryRequest;
using GRPCConnection::QueryResponse;
using GRPCConnection::GRPC;
namespace DB
{
class CommonCallData
{
     public:
     GRPC::AsyncService* Service;
     grpc::ServerCompletionQueue* notification_cq;
     grpc::ServerCompletionQueue* new_call_cq;
     grpc::ServerContext gRPCcontext;
     IServer* iServer;
     bool with_stacktrace = false;
     Poco::Logger * log;
     std::unique_ptr<CommonCallData> next_client;

     public:
          explicit CommonCallData(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* notification_cq_, grpc::ServerCompletionQueue* new_call_cq_, IServer* iServer_, Poco::Logger * log_)
               : Service(Service_), notification_cq(notification_cq_), new_call_cq(new_call_cq_), iServer(iServer_), log(log_)
               {}
          virtual ~CommonCallData() {}
          virtual void respond() = 0;
};

class CallDataQuery : public CommonCallData {
     public:
          CallDataQuery(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* notification_cq_, grpc::ServerCompletionQueue* new_call_cq_, IServer* server_, Poco::Logger * log_)
          : CommonCallData(Service_, notification_cq_, new_call_cq_, server_, log_), responder(&gRPCcontext), context(iServer->context()), pool(1) {
               out = std::make_shared<WriteBufferFromGRPC>(&responder, (void*)this, nullptr);
               Service->RequestQuery(&gRPCcontext, &request, &responder, new_call_cq, notification_cq, this);
          }
          void ParseQuery();
          void ExecuteQuery();
          void ProgressQuery();
          void FinishQuery();

          bool senData(const Block & block);
          bool sendProgress();

          virtual void respond() override
          {
               try {
                    if (!out->onProgress() && !out->isFinished())
                    {
                         new CallDataQuery(Service, notification_cq, new_call_cq, iServer, log);
                         ParseQuery();
                         ExecuteQuery();
                    } else if (out->onProgress() && !out->isFinished()) {
                         ProgressQuery();
                    } else if (out->isFinished()) {
                         delete this;
                    }
                         
               } 
               catch(...) {
                    tryLogCurrentException(log);
                    std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
                    int exception_code = getCurrentExceptionCode();
                    response.set_exception_occured(exception_message);
                    out->setFinish(true);
                    responder.WriteAndFinish(response, grpc::WriteOptions(), grpc::Status(), (void*)this);
               }
          }
          virtual ~CallDataQuery() override
          {
               if (lazy_format) {
                  lazy_format->finish();
               }
               try
               {
                    pool.wait();
               }
               catch (...)
               {
                    //
               }
               query_context.reset();
               query_scope.reset();
          }
          
     private:
          QueryRequest request;
          QueryResponse response;
          grpc::ServerAsyncWriter<QueryResponse> responder;

          Stopwatch progress_watch;
          Stopwatch query_watch;
          Progress progress;

          std::shared_ptr<LazyOutputFormat> lazy_format;
          BlockIO io;
          PipelineExecutorPtr executor;
          Context context;
          ThreadPool pool;
          std::atomic_bool exception = false;
          std::optional<Context> query_context;

          std::shared_ptr<WriteBufferFromGRPC> out;
          std::optional<CurrentThread::QueryScope> query_scope;


};

class GRPCServer final : public Poco::Runnable {
     public:
     GRPCServer(const GRPCServer &handler) = delete;
     GRPCServer(GRPCServer &&handler) = delete;
     GRPCServer(std::string server_address_, IServer & server_) : iServer(server_), log(&Poco::Logger::get("GRPCHandler")) {
          grpc::ServerBuilder builder;
          builder.AddListeningPort(server_address_, grpc::InsecureServerCredentials());
          //keepalive pings default values
          builder.RegisterService(&Service);
          builder.SetMaxReceiveMessageSize(INT_MAX);
          notification_cq = builder.AddCompletionQueue();
          new_call_cq = builder.AddCompletionQueue();
          Server = builder.BuildAndStart();
     }
     void stop() {
          Server->Shutdown();
          notification_cq->Shutdown();
          new_call_cq->Shutdown();
     }
     virtual void run() override {
          HandleRpcs();
     }
     void HandleRpcs() {
          new CallDataQuery(&Service, notification_cq.get(), new_call_cq.get(), &iServer, log);
    
          // rpc event "read done / write done / close(already connected)" call-back by this completion queue
          auto handle_calls_completion = [&]()
          {
               void* tag;
               bool ok;
               while (true)
               {
                    
                    GPR_ASSERT(new_call_cq->Next(&tag, &ok));
                    if (!ok) {
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
                    if (!ok) {
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
          GRPC::AsyncService Service;
          std::unique_ptr<grpc::Server> Server;
          std::string server_address;
};

 }
