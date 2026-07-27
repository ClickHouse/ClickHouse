#pragma once

#include <base/types.h>

namespace Poco::Net
{
    class StreamSocket;
}

namespace DB
{

class ReadBuffer;
class WriteBuffer;

namespace StreamingExchangeProtocol
{
    /// Wire-format version, exchanged in SourceHello/SinkHello and required to match
    /// exactly on both ends. Bumped on any change to packet layouts. Version 3 carries a
    /// `jwt_token` in `SourceHelloBody` for authenticating the connecting source (empty
    /// when authentication is not used).
    static constexpr UInt64 PROTOCOL_VERSION = 3;

    /// Sanity cap for the body of a Hello packet.
    static constexpr UInt64 MAX_HELLO_BODY_BYTES = 64 * 1024;

    /// Sanity cap for the body of a Data packet. The peer-supplied size is allocated
    /// before any payload is read, so an unbounded value would let a buggy or hostile
    /// sink make the source allocate arbitrarily large buffers.
    static constexpr UInt64 MAX_DATA_PACKET_BODY_BYTES = 256ULL * 1024 * 1024;

    /// Per-recv/send timeout applied to the Hello exchange on the server side.
    /// The handshake runs inline on the accept thread, so a silent peer must not
    /// hold it indefinitely.
    static constexpr int HELLO_TIMEOUT_SECONDS = 10;

    /// Packet types between StreamingExchangeSink and StreamingExchangeSource.
    /// SourceHello/SinkHello magic numbers were changed when framing was introduced,
    /// so peers on the older unframed handshake fail with UNEXPECTED_PACKET_FROM_CLIENT.
    enum PacketType : UInt64
    {
        SourceHello = 0x0004e110,   /// Sent by source to sink when initiating connection
        SinkHello   = 0x1004e110,   /// Response from sink to source
        Data        = 0x0000da7a,   /// Data packet
        NoMoreDataNeeded  = 0x00a11fed, /// Sent by source to sink when no more data is needed; not framed by PacketHeader
    };

    /// Packet header used by Data, SourceHello, and SinkHello packets. Contains size in bytes
    /// of the whole serialized body. This allows to first read full packet from the socket with
    /// non-blocking reads and epoll and then deserialize the body from memory buffer to also avoid blocking.
    struct PacketHeader
    {
        UInt64 packet_type;
        UInt64 bytes_size;  /// Size of the packet body (does not include this header)
    };

    /// Wire format of the SourceHello body. The version is read first and must match this
    /// node's `PROTOCOL_VERSION`; the rest of the body is parsed only after that check, so a
    /// mismatched peer never has its (possibly differently-laid-out) body parsed further.
    struct SourceHelloBody
    {
        UInt64 source_version = 0;
        String query_id;
        String stream_name;
        /// Bearer JWT for authenticating the source; empty when the source has no token.
        String jwt_token;

        static UInt64 readVersion(ReadBuffer & in);
        void readAfterVersion(ReadBuffer & in);
        void write(WriteBuffer & out) const;
    };

    /// Wire format of the SinkHello body. Currently carries the sink's protocol version
    /// so the source can produce a precise diagnostic on a mismatch.
    struct SinkHelloBody
    {
        UInt64 sink_version = 0;

        void read(ReadBuffer & in);
        void write(WriteBuffer & out) const;
    };

    /// Single receive that retries on EINTR. Returns bytes read, or 0 if the socket
    /// would block. Throws Poco::Net::NetException on early EOF or other socket error;
    /// `description` labels the call site in the exception message.
    ssize_t tryReceive(Poco::Net::StreamSocket & socket, char * buffer, size_t size, const String & description);
}
}
