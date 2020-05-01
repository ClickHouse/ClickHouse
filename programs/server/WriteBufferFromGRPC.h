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

    void nextImpl() override {
        written = true;
        progress = true;
        QueryResponse response;
        String buffer(working_buffer.begin(), working_buffer.begin() + offset());
        if (buffer.empty()) {
            written = false;
        }
        response.set_progress_tmp(buffer);

        responder->Write(response, tag);
    }

public:
    WriteBufferFromGRPC(grpc::ServerAsyncWriter<QueryResponse>* responder_, void* tag_) : responder(responder_), tag(tag_) {}

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
    void finalize() override {
        progress = false;
        finished = true;
        responder->Finish(grpc::Status(), tag);
    }
};

}