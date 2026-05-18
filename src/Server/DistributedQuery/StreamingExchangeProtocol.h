#pragma once

#include <base/types.h>

namespace DB
{
namespace StreamingExchangeProtocol
{
    /// Wire-format version. Bumped on any change to packet layouts.
    /// Negotiated in SourceHello/SinkHello; mismatches reject the connection.
    static constexpr UInt64 PROTOCOL_VERSION = 1;

    /// Sanity cap for the body of a Hello packet.
    static constexpr UInt64 MAX_HELLO_BODY_BYTES = 64 * 1024;

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
}
}
