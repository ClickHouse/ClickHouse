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

    bool hasBufferedData() const { return hasPendingData() || buffer_socket.hasPendingData(); }

    Poco::Net::SocketAddress peerAddress() { return peer_address; }
    Poco::Net::SocketAddress ourAddress() { return our_address; }

protected:
    bool startChunk();
    bool nextChunk();
    bool nextImpl() override;

private:
    LoggerPtr log;
    Poco::Net::SocketAddress peer_address;
    Poco::Net::SocketAddress our_address;
    ReadBufferFromPocoSocket buffer_socket;
    bool chunked = false;
    UInt32 chunk_left = 0; // chunk left to read from socket
    bool started = false;
};

}
