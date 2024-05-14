#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>

namespace DB
{

class ReadBufferFromPocoSocketChunked: public ReadBuffer
{
public:
    explicit ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    explicit ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void enableChunked();
    bool poll(size_t timeout_microseconds);
    void setAsyncCallback(AsyncCallback async_callback_);

protected:
    bool startChunk();
    bool nextChunk();
    bool nextImpl() override;

private:
    LoggerPtr log;
    ReadBufferFromPocoSocket buffer_socket;
    bool chunked = false;
    UInt32 chunk_left = 0; // chunk left to read from socket
    UInt8 skip_next = 0; // skip already processed bytes in buffer_socket
    bool started = false;
};

}
