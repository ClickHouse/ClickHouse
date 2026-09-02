#pragma once

#include <Common/logger_useful.h>
#include <IO/WriteBufferFromPocoSocket.h>


namespace DB
{

class WriteBufferFromPocoSocketChunked: public WriteBufferFromPocoSocket
{
public:
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    explicit WriteBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & write_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    WriteBufferFromPocoSocketChunked(
        Poco::Net::Socket & socket_,
        const ProfileEvents::Event & write_event_,
        const ProfileEvents::Event & flush_event_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void enableChunked();
    void finishChunk();

    /// Size of the chunk size holder the buffer reserves at its start after every flush. Those
    /// bytes are framing rather than part of any packet written through the buffer.
    size_t reservedChunkHeaderSize() const { return chunked ? sizeof(*chunk_size_ptr) : 0; }

protected:
    void nextImpl() override;
    void finalizeImpl() override;
    Poco::Net::SocketAddress peerAddress() const { return peer_address; }
    Poco::Net::SocketAddress ourAddress() const { return our_address; }

private:
    LoggerPtr log;
    bool chunked = false;
    UInt32 * last_finish_chunk = nullptr;       // pointer to the last chunk header created by finishChunk
    bool chunk_started = false;                 // chunk started flag
    UInt32 * chunk_size_ptr = nullptr;          // pointer to the chunk size holder in the buffer
    size_t finishing = sizeof(*chunk_size_ptr); // indicates not enough buffer for end-of-chunk marker
    ProfileEvents::Event flush_event = ProfileEvents::end();
};

}
