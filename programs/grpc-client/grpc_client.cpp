#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "GrpcConnection.grpc.pb.h"

class GRPCClient {
    public:
        explicit GRPCClient(std::shared_ptr<grpc::Channel> channel)
            : stub_(GRPCConnection::GRPC::NewStub(channel)) {}
         std::string Query(const std::string& query) {
            GRPCConnection::QueryRequest request;
            GRPCConnection::QueryResponse reply;
            grpc::Status status;

            GRPCConnection::User userInfo;
            userInfo.set_user("default");
            userInfo.set_key("");
            userInfo.set_quota("default");

            request.set_allocated_user_info(&userInfo);
            // interactive_delay in miliseconds
            request.set_interactive_delay(1000);
            GRPCConnection::QuerySettings querySettigs;
            querySettigs.set_query(query);

            int id = rand();

            querySettigs.set_query_id(std::to_string(id));
            
            request.set_allocated_query_info(&querySettigs);

            grpc::ClientContext context;
            
            void* got_tag = (void*)1;
            bool ok = false;
            
            std::unique_ptr<grpc::ClientReader<GRPCConnection::QueryResponse> > reader(stub_->Query(&context, request));
            // if (id % 3 == 0) {
            //     request.release_query_info();
            //     request.release_user_info();
            //     return "Fail check";
            // }
            while (reader->Read(&reply)) {
                // if (!reply.progress().read_rows()) {
                //     std::cout << "Progress " << id<< ":{\n" << "read_rows: "            << reply.progress().read_rows() << '\n'
                //                                             << "read_bytes: "           << reply.progress().read_bytes() << '\n'
                //                                             << "total_rows_to_read: "   << reply.progress().total_rows_to_read() << '\n'
                //                                             << "written_rows: "         << reply.progress().written_rows() << '\n'
                //                                             << "written_bytes: "        << reply.progress().written_bytes() << '\n';


                // }

                if (!reply.query().empty()) {
                    std::cout << "Query Part:\n " << id<< reply.query()<<'\n';
                } else if (!reply.progress_tmp().empty()) {
                    std::cout << "Progress:\n " << id<< reply.progress_tmp() <<'\n';
                }
            }

            request.release_query_info();
            request.release_user_info();

            if (status.ok() && reply.exception_occured().empty()) {
                return "";
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
    std::cout << client.Query("CREATE TABLE t (a UInt8) ENGINE = Memory") << std::endl;
    std::cout << client.Query("CREATE TABLE t (a UInt8) ENGINE = Memory") << std::endl;
    std::cout << client.Query("INSERT INTO t VALUES") << std::endl;
    std::cout << client.Query("INSERT INTO t VALUES (1),(2),(3)") << std::endl;
    std::cout << client.Query("INSERT INTO t VALUES (4),(5),(6)") << std::endl;
    std::cout << client.Query("INSERT INTO t FORMAT Values (7),(8),(9) ") << std::endl;
    std::cout << client.Query("SELECT count() FROM numbers(1)") << std::endl;
    std::cout << client.Query("INSERT INTO t FORMAT TabSeparated 10\n11\n12\n") << std::endl;
    std::cout << client.Query("SELECT a FROM t ORDER BY a") << std::endl;
    std::cout << client.Query("DROP TABLE t") << std::endl;
    std::cout << client.Query("SELECT 100") << std::endl;
    std::cout << client.Query("SELECT count() FROM numbers(10000000000)") << std::endl;
    std::cout << client.Query("SELECT count() FROM numbers(100)") << std::endl;
    std::cout << client.Query("WITH ['hello'] AS hello SELECT hello, * FROM ( WITH ['hello'] AS hello SELECT hello)");
    std::cout << client.Query("CREATE TABLE arrays_test (s String, arr Array(UInt8)) ENGINE = Memory;") << std::endl;
    std::cout << client.Query("INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);") << std::endl;
    std::cout << client.Query("SELECT s FROM arrays_test") << std::endl;
    std::cout << client.Query("") << std::endl;

    return 0;
}