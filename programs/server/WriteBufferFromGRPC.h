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

    grpc::ServerAsyncWriter<QueryResponse>* responder;
    void* tag;

    bool progress = false;
    bool finished = false;
    bool written = true;
    std::function<QueryResponse(const String& buffer)> setResposeDetails;


    void nextImpl() override {
        written = true;
        progress = true;
        
        String buffer(working_buffer.begin(), working_buffer.begin() + offset());
        if (buffer.empty()) {
            written = false;
        }
        auto response = setResposeDetails(buffer);
        responder->Write(response, tag);
    }

public:
    WriteBufferFromGRPC(grpc::ServerAsyncWriter<QueryResponse>* responder_, void* tag_, std::function<QueryResponse(const String& buffer)> setResposeDetails_) : responder(responder_), tag(tag_), setResposeDetails(setResposeDetails_) {}

    ~WriteBufferFromGRPC() override {}
    bool onProgress() {
        return progress;
    }
    bool isFinished() {
        return finished;
    }
    bool isWritten() {
        return written;
    }
    void setFinish(bool fl) {
        finished = fl;
    }
    void setResponse(std::function<QueryResponse(const String& buffer)> function) {
        setResposeDetails = function;
    }
    void finalize() override {
        progress = false;
        finished = true;
        responder->Finish(grpc::Status(), tag);
    }
};

}