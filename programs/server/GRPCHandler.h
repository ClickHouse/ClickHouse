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
#include <Processors/Formats/LazyOutputFormat.h>
 #include <grpc++/server.h>
 #include <grpc++/server_builder.h>
 #include <grpc++/server_context.h>
 #include "GrpcConnection.grpc.pb.h"

 using GRPCConnection::HelloRequest;
 using GRPCConnection::HelloResponse;
 using GRPCConnection::QueryRequest;
 using GRPCConnection::QueryResponse;
 using GRPCConnection::GRPC;

 namespace DB
 {
 class CommonCallData
 {
     public:
     GRPC::AsyncService* Service;
     grpc::ServerCompletionQueue* CompilationQueue;
     grpc::ServerContext gRPCcontext;
     
     IServer* iServer;
     bool with_stacktrace = false;
     enum CallStatus { CREATE, PREPARE, PROCESS, ONFINISH, FINISH };
     CallStatus status;
     Poco::Logger * log;

     public:
          explicit CommonCallData(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* CompilationQueue_, IServer* iServer_, Poco::Logger * log_)
               : Service(Service_), CompilationQueue(CompilationQueue_), status(CREATE),iServer(iServer_), log(log_)
               {}
          virtual ~CommonCallData() {}
          virtual void Proceed(bool ok) = 0;
          virtual void HandleExceptions(const std::string& ex) = 0;
          virtual void Run() {
               try 
               {
                    Proceed(true);
               }
               catch (...) {
                    tryLogCurrentException(log);
                    std::string exception_message = getCurrentExceptionMessage(with_stacktrace, true);
                    int exception_code = getCurrentExceptionCode();
                    HandleExceptions(exception_message);
               }
          }
 };


 class CallDataHello : public CommonCallData {
     public:
          CallDataHello(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* CompilationQueue_, IServer* server_, Poco::Logger * log_)
          : CommonCallData(Service_, CompilationQueue_, server_, log_), responder(&gRPCcontext) {
               Proceed(true);
          }
          void Proceed(bool ok);
          void HandleExceptions(const std::string& ex){}
     private:
          HelloRequest request;
          HelloResponse response;
          grpc::ServerAsyncResponseWriter<HelloResponse> responder;
 };

 class CallDataQuery : public CommonCallData {
     public:
          CallDataQuery(GRPC::AsyncService* Service_, grpc::ServerCompletionQueue* CompilationQueue_, IServer* server_, Poco::Logger * log_)
          : CommonCallData(Service_, CompilationQueue_, server_, log_), responder(&gRPCcontext), new_responder_created(false), context(iServer->context()), pool(1) {
               Proceed(true);
          }
          void Proceed(bool ok);
          void HandleExceptions(const std::string& ex) {
              response.set_exception_occured(ex);
              status = FINISH;
              responder.WriteAndFinish(response, grpc::WriteOptions(), grpc::Status(), (void*)this);
          }
          void Execute();
     private:
          bool new_responder_created;
          QueryRequest request;
          QueryResponse response;
          grpc::ServerAsyncWriter<QueryResponse> responder;
          // std::unique_ptr<WriteBufferFromString> used_output;
          Stopwatch progress_watch;
          std::shared_ptr<LazyOutputFormat> lazy_format;
          BlockIO io;
          PipelineExecutorPtr executor;
          Context context;
          ThreadPool pool;
          std::atomic_bool exception = false;
 };

 class GRPCServer final : public Poco::Runnable {
     public:
     GRPCServer(const GRPCServer &handler) = delete;
     GRPCServer(GRPCServer &&handler) = delete;
     GRPCServer(std::string server_address_, IServer& server_) : iServer(server_), log(&Poco::Logger::get("GRPCHandler")), server_address(server_address_) {
          grpc::ServerBuilder builder;
          builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
          //keepalive pings default values
          builder.RegisterService(&Service);
          builder.SetMaxReceiveMessageSize(INT_MAX);
          CompilationQueue = builder.AddCompletionQueue();
          Server = builder.BuildAndStart();
     }
     void stop() {
          Server->Shutdown();
          CompilationQueue->Shutdown();
     }
     virtual void run() override {
          HandleRpcs();
     }
     void HandleRpcs() {
          // ThreadStatus thread_status;
          new CallDataHello(&Service, CompilationQueue.get(), &iServer, log);
          new CallDataQuery(&Service, CompilationQueue.get(), &iServer, log);
          void* tag;
          bool ok;
          while (true) {
          GPR_ASSERT(CompilationQueue->Next(&tag, &ok));
          GPR_ASSERT(ok);
          static_cast<CommonCallData*>(tag)->Run();
          }
     }
     
     private:
          IServer & iServer;
          Poco::Logger * log;
          std::unique_ptr<grpc::ServerCompletionQueue> CompilationQueue;
          GRPC::AsyncService Service;
          std::unique_ptr<grpc::Server> Server;
          std::string server_address;
 };


 }