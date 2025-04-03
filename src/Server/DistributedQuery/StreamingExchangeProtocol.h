#pragma once

namespace DB
{
namespace StreamingExchangeProtocol
{
    /// Packet types between StreamingExchangeSink and StreamingExchangeSource.
    enum PacketType
    {
        SinkHello   = 0x5104e110,   /// Sent by sink to source when initiating connection
        SourceHello = 0x5004e110,   /// Response from source to sink
        Data        = 0x0000da7a,   /// Data packet
    };
}
}
