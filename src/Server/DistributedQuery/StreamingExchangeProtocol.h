#pragma once

#include <base/types.h>

namespace DB
{
namespace StreamingExchangeProtocol
{
    /// Packet types between StreamingExchangeSink and StreamingExchangeSource.
    enum PacketType
    {
        SourceHello = 0x5004e110,   /// Sent by source to sink when initiating connection
        SinkHello   = 0x5104e110,   /// Response from sink to source
        Data        = 0x0000da7a,   /// Data packet
        NoMoreDataNeeded  = 0x00a11fed, /// Sent by source to sink when no more data is needed
    };

    /// Data packet starts with a header that contains size in bytes of the whole serialized data chunk.
    /// This allows to first read full packet form the socket with non-blocking reads and epoll and then
    /// deserialize the chunk from memory buffer to also avoid blocking.
    struct DataPacketHeader
    {
        UInt64 packet_type; /// Should be 'Data'
        UInt64 bytes_size;  /// Size of the packet body (does not include this header)
    };
}
}
