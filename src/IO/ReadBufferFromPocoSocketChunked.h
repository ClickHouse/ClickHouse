#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromPocoSocket.h>

/*

Handshake              +=============
                       | 'Hello' type
                       |   handshake exchange
                       |     chunked protocol negotiation
                       +=============


Basic chunk:
                       +=============
Chunk begins           | 0x12345678      chunk size, 4 bytes little endian
                       +-------------
                       | Packet type     always follows beginning of the chunk
                       |   packet data
                       +-------------
Chunk ends             | 0x00000000      4 zero bytes
                       +=============


Datastream chunk:
                       +=============
Chunk begins           | 0x12345678
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
...arbitrary number        .....
of packets...              .....
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
Chunk ends             | 0x00000000
                       +=============


Multipart chunk:
                       +=============
Chunk begins           | 0x12345678      chunk part size, 4 bytes little endian
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
                       | Packet type
                       |   (partial) packet data
                       +=============
Chunk continues        | 0x12345678      chunk next part size, 4 bytes little endian
                       +=============
                       |   possibly previous packet's data
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
...arbitrary number        .....
of chunk parts...          .....
                       +-------------
                       | Packet type
                       |   packet data
                       +-------------
Chunk ends             | 0x00000000
                       +=============

*/

namespace DB
{

class ReadBufferFromPocoSocketChunked: public ReadBufferFromPocoSocketBase
{
public:
    using ReadBufferFromPocoSocketBase::setAsyncCallback;

    explicit ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);
    explicit ReadBufferFromPocoSocketChunked(Poco::Net::Socket & socket_, const ProfileEvents::Event & read_event_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    void enableChunked();

    bool hasBufferedData() const;

    /// Keep the unread bytes and append after them at most `max_bytes` next bytes of the stream: the bytes that are
    /// already buffered (with the chunked protocol, the received bytes of the next chunks are buffered with their
    /// framing) and, if `read_socket`, the bytes of one read of the socket. With the chunked protocol, the framing is
    /// removed, so the appended bytes follow the unread ones as if they were in the same chunk. It does not wait if
    /// `read_socket` is false or the socket has already received some bytes. Returns false at the end of the stream.
    bool appendWithoutWaiting(size_t max_bytes, bool read_socket);

    bool poll(size_t timeout_microseconds) override;

    Poco::Net::SocketAddress peerAddress() { return peer_address; }
    Poco::Net::SocketAddress ourAddress() { return our_address; }

protected:
    bool loadNextChunk(Position c_pos, bool cont = false);
    bool processChunkLeft(Position c_pos);
    bool nextImpl() override;

    /// `appendWithoutWaiting` with the chunked protocol.
    bool appendChunkedWithoutWaiting(size_t max_bytes, bool read_socket);

    /// Whether some bytes of the stream are buffered after the working buffer.
    bool hasBufferedChunkData() const;

    Poco::Net::SocketAddress our_address;

private:
    LoggerPtr log;
    Position data_end = nullptr; // end position of data in the internal_buffer
    UInt32 chunk_left = 0;       // chunk left to read from socket
    UInt32 next_chunk = 0;       // size of the next cnunk
    UInt8 chunked = 0;           // 0 - disabled; 1 - started; 2 - enabled;
};

}
