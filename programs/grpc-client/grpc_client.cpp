#include <iostream>
#include <memory>
#include <string>

#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "GrpcConnection.grpc.pb.h"

class GRPCClient {
    public:
        explicit GRPCClient(std::shared_ptr<grpc::Channel> channel)
            : stub_(GRPCConnection::GRPC::NewStub(channel)) {}
        std::string SayHello(const std::string& user) {
            GRPCConnection::HelloRequest request;
            request.set_username(user);

            GRPCConnection::HelloResponse reply;
            grpc::ClientContext context;
            grpc::CompletionQueue cq;
            grpc::Status status;
            std::unique_ptr<grpc::ClientAsyncResponseReader<GRPCConnection::HelloResponse> > rpc(
                stub_->PrepareAsyncSayHello(&context, request, &cq));

            rpc->StartCall();
            rpc->Finish(&reply, &status, (void*)1);

            void* got_tag;
            bool ok = false;
            GPR_ASSERT(cq.Next(&got_tag, &ok));
            GPR_ASSERT(got_tag == (void*)1);
            GPR_ASSERT(ok);
            if (status.ok()) {
                return reply.response();
            } else {
                return "RPC failed";
            }
         }
         std::string Query(const std::string& query) {
            GRPCConnection::QueryRequest request;
            GRPCConnection::QueryResponse reply;
            grpc::Status status;
            request.set_query(query);
            request.set_x_clickhouse_user("default");
            request.set_x_clickhouse_key("default");
            request.set_x_clickhouse_quota("default");
            request.set_query_id(query+"123");
            request.set_interactive_delay(1000);

            grpc::ClientContext context;
            
            void* got_tag = (void*)1;
            bool ok = false;
            
            status = stub_->Query(&context, request, &reply);
            if (status.ok() && reply.exception_occured().empty()) {
                return reply.query_id();
            } else if (status.ok() && !reply.exception_occured().empty()) {
                return reply.exception_occured();
            } else {
                return "RPC failed";
            }
         }

    private:
        std::unique_ptr<GRPCConnection::GRPC::Stub> stub_;
 };

int main(int argc, char** argv) {
    std::cout << "Try: " << argv[1] << std::endl;
    grpc::ChannelArguments ch_args;
    ch_args.SetMaxReceiveMessageSize(-1);
    GRPCClient client(
     grpc::CreateCustomChannel(argv[1], grpc::InsecureChannelCredentials(), ch_args));
    std::cout << client.SayHello("hello") << std::endl;
    std::cout << client.Query("CREATE TABLE t (a UInt8) ENGINE = Memory") << std::endl;
    std::cout << client.Query("INSERT INTO t VALUES (1),(2),(3)") << std::endl;
    std::cout << client.Query("INSERT INTO t VALUES (4),(5),(6)") << std::endl;
    std::cout << client.Query("INSERT INTO t FORMAT Values (7),(8),(9) ") << std::endl;
    std::cout << client.Query("INSERT INTO t FORMAT TabSeparated 10\n11\n12\n") << std::endl;
    std::cout << client.Query("SELECT a FROM t ORDER BY a") << std::endl;
    std::cout << client.Query("DROP TABLE t") << std::endl;
    std::cout << client.Query("SELECT count() FROM numbers(10000000000)") << std::endl;

    return 0;
}
