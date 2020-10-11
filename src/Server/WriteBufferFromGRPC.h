#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include "GrpcConnection.grpc.pb.h"
#include <grpc++/server.h>

using GRPCConnection::QueryRequest;
using GRPCConnection::QueryResponse;
using GRPCConnection::GRPC;

namespace DB
{

class WriteBufferFromGRPC : public BufferWithOwnMemory<WriteBuffer>
{
protected:

    grpc::ServerAsyncReaderWriter<QueryResponse, QueryRequest>* responder;
    void* tag;

    bool progress = false;
    bool finished = false;
    std::function<QueryResponse(const String& buffer)> setResposeDetails;


    void nextImpl() override
    {
        progress = true;

        String buffer(working_buffer.begin(), working_buffer.begin() + offset());
        auto response = setResposeDetails(buffer);
        responder->Write(response, tag);
    }

public:
    WriteBufferFromGRPC(grpc::ServerAsyncReaderWriter<QueryResponse, QueryRequest>* responder_, void* tag_, std::function<QueryResponse(const String& buffer)> setResposeDetails_)
    : responder(responder_), tag(tag_), setResposeDetails(setResposeDetails_)
    {}

    ~WriteBufferFromGRPC() override {}
    bool onProgress()
    {
        return progress;
    }
    bool isFinished()
    {
        return finished;
    }
    void setFinish(bool fl)
    {
        finished = fl;
    }
    void setResponse(std::function<QueryResponse(const String& buffer)> function)
    {
        setResposeDetails = function;
    }
    void finalize() override
    {
        progress = false;
        finished = true;
        responder->Finish(grpc::Status(), tag);
    }
};

}