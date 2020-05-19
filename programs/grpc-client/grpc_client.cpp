#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <stdlib.h>
#include <grpc++/channel.h>
#include <grpc++/client_context.h>
#include <grpc++/create_channel.h>
#include <grpc++/security/credentials.h>
#include "GrpcConnection.grpc.pb.h"

class GRPCClient
{
    public:
        explicit GRPCClient(std::shared_ptr<grpc::Channel> channel)
            : stub_(GRPCConnection::GRPC::NewStub(channel))
            {}
         std::string Query(const GRPCConnection::User& userInfo,
                            const std::string& query,
                            std::vector<std::string> insert_data = {})
         {
            GRPCConnection::QueryRequest request;
            grpc::Status status;
            GRPCConnection::QueryResponse reply;
            grpc::ClientContext context;
            auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(10000);
            context.set_deadline(deadline);

            auto user = std::make_unique<GRPCConnection::User>(userInfo);
            auto querySettigs = std::make_unique<GRPCConnection::QuerySettings>();
            int id = rand();
            request.set_allocated_user_info(user.release());
            // interactive_delay in miliseconds
            request.set_interactive_delay(1000);

            querySettigs->set_query(query);
            querySettigs->set_format("Values");
            querySettigs->set_query_id(std::to_string(id));
            querySettigs->set_data_stream((insert_data.size() != 0));
            (*querySettigs->mutable_settings())["max_query_size"] ="100";


            request.set_allocated_query_info(querySettigs.release());

            void* got_tag = (void*)1;
            bool ok = false;

            std::unique_ptr<grpc::ClientReaderWriter<GRPCConnection::QueryRequest, GRPCConnection::QueryResponse> > reader(stub_->Query(&context));
            reader->Write(request);

            auto write = [&reply, &reader, &insert_data]()
            {
                GRPCConnection::QueryRequest request_insert;
                for (const auto& data : insert_data)
                {
                    request_insert.set_insert_data(data);
                    if (reply.exception_occured().empty())
                    {
                        reader->Write(request_insert);
                    }
                    else
                    {
                        break;
                    }
                }
                request_insert.set_insert_data("");
                if (reply.exception_occured().empty())
                {
                    reader->Write(request_insert);
                }
                // reader->WritesDone();
            };
            std::thread write_thread(write);
            write_thread.detach();

            while (reader->Read(&reply))
            {

                if (!reply.output().empty())
                {
                    std::cout << "Query Part:\n " << id<< reply.output()<<'\n';
                }
                else if (reply.progress().read_rows()
                        || reply.progress().read_bytes()
                        || reply.progress().total_rows_to_read()
                        || reply.progress().written_rows()
                        || reply.progress().written_bytes())
                {
                    std::cout << "Progress " << id<< ":{\n" << "read_rows: "            << reply.progress().read_rows() << '\n'
                                                            << "read_bytes: "           << reply.progress().read_bytes() << '\n'
                                                            << "total_rows_to_read: "   << reply.progress().total_rows_to_read() << '\n'
                                                            << "written_rows: "         << reply.progress().written_rows() << '\n'
                                                            << "written_bytes: "        << reply.progress().written_bytes() << '\n';


                }
                else if (!reply.totals().empty())
                {
                    std::cout << "Totals:\n " << id << " " << reply.totals() <<'\n';
                }
                else if (!reply.extremes().empty())
                {
                    std::cout << "Extremes:\n " << id << " " << reply.extremes() <<'\n';
                }
            }

            if (status.ok() && reply.exception_occured().empty())
            {
                return "";
            }
            else if (status.ok() && !reply.exception_occured().empty())
            {
                return reply.exception_occured();
            }
            else
            {
                return "RPC failed";
            }
         }

    private:
        std::unique_ptr<GRPCConnection::GRPC::Stub> stub_;
};

int main(int argc, char** argv)
{
    GRPCConnection::User userInfo1;
    userInfo1.set_user("default");
    userInfo1.set_password("");
    userInfo1.set_quota("default");

    std::cout << "Try: " << argv[1] << std::endl;
    grpc::ChannelArguments ch_args;
    ch_args.SetMaxReceiveMessageSize(-1);
    GRPCClient client(
     grpc::CreateCustomChannel(argv[1], grpc::InsecureChannelCredentials(), ch_args));
    {
        std::cout << client.Query(userInfo1, "CREATE TABLE t (a UInt8) ENGINE = Memory") << std::endl;
        std::cout << client.Query(userInfo1, "CREATE TABLE t (a UInt8) ENGINE = Memory") << std::endl;
        std::cout << client.Query(userInfo1, "INSERT INTO t VALUES", {"(1),(2),(3)", "(4),(6),(5)"}) << std::endl;
        std::cout << client.Query(userInfo1, "INSERT INTO t_not_defined VALUES", {"(1),(2),(3)", "(4),(6),(5)"}) << std::endl;
        std::cout << client.Query(userInfo1, "SELECT a FROM t ORDER BY a") << std::endl;
        std::cout << client.Query(userInfo1, "DROP TABLE t") << std::endl;
    }
    {
        std::cout << client.Query(userInfo1, "SELECT count() FROM numbers(1)") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT 100") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT count() FROM numbers(10000000000)") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT count() FROM numbers(100)") << std::endl;
    }
    {
        std::cout << client.Query(userInfo1, "CREATE TABLE arrays_test (s String, arr Array(UInt8)) ENGINE = Memory;") << std::endl;
        std::cout << client.Query(userInfo1, "INSERT INTO arrays_test VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT s FROM arrays_test") << std::endl;
        std::cout << client.Query(userInfo1, "DROP TABLE arrays_test") << std::endl;
        std::cout << client.Query(userInfo1, "") << std::endl;
    }

    {//Check null return from pipe
        std::cout << client.Query(userInfo1, "CREATE TABLE table2 (x UInt8, y UInt8) ENGINE = Memory;") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT x FROM table2") << std::endl;
        std::cout << client.Query(userInfo1, "DROP TABLE table2") << std::endl;
    }
    {//Check Totals
        std::cout << client.Query(userInfo1, "CREATE TABLE tabl (x UInt8, y UInt8) ENGINE = Memory;") << std::endl;
        std::cout << client.Query(userInfo1, "INSERT INTO tabl VALUES (1, 2), (2, 4), (3, 2), (3, 3), (3, 4);") << std::endl;
        std::cout << client.Query(userInfo1, "SELECT sum(x), y FROM tabl GROUP BY y WITH TOTALS") << std::endl;
        std::cout << client.Query(userInfo1, "DROP TABLE tabl") << std::endl;
    }

    return 0;
}
