#include "Storages/StorageS3ReaderFollower.h"

#include <grpcpp/channel.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include "Common/Throttler.h"
#include "clickhouse_s3_reader.grpc.pb.h"


#include <memory>
#include <string>

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using S3TaskServer = clickhouse::s3_reader::S3TaskServer;
using S3TaskRequest = clickhouse::s3_reader::S3TaskRequest;
using S3TaskReply = clickhouse::s3_reader::S3TaskReply;



namespace DB
{


namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

class S3TaskManagerClient
{   
public:
    explicit S3TaskManagerClient(std::shared_ptr<grpc::Channel> channel)
        : stub(S3TaskServer::NewStub(channel))
    {}
    

    [[ maybe_unused ]] std::string GetNext(const std::string & query_id)
    {
        S3TaskRequest request;
        request.set_query_id(query_id);

        S3TaskReply reply;
        ClientContext context;

        Status status = stub->GetNext(&context, request, &reply);

        if (status.ok()) {
            return reply.message();
        } else {
            throw Exception("RPC Failed", ErrorCodes::LOGICAL_ERROR);
        }

    }
private:
    std::unique_ptr<S3TaskServer::Stub> stub;
};

}

const auto * target_str = "localhost:50051";

class StorageS3ReaderFollower::Impl
{
public:
    Impl() 
    : client(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())) {
    }
    void startup();
private:
    S3TaskManagerClient client;
};

void StorageS3ReaderFollower::Impl::startup() {

}


void StorageS3ReaderFollower::startup()
{
    return pimpl->startup();
}

bool StorageS3ReaderFollower::isRemote() const
{
    return true;
}

std::string StorageS3ReaderFollower::getName() const
{
    return "StorageS3ReaderFollower";
}

}

