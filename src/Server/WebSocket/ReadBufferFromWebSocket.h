#pragma once

#include <IO/ReadBuffer.h>
#include <IO/BufferBase.h>
#include <IO/BufferWithOwnMemory.h>
#include <Server/WebSocket/WebSocket.h>

namespace DB
{

class ReadBufferFromWebSocket : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ReadBufferFromWebSocket(WebSocket & ws_) : ws(ws_), poco_buffer(0){}

private:

    WebSocket & ws;
    Poco::Buffer<char> poco_buffer;

    bool nextImpl() override;

    bool GetNextMessageFromWebSocket();

    bool last_message_recived = false;
};

}
