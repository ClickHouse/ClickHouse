#pragma once

namespace DB
{
namespace StreamingExchangeProtocol
{
    /// Packet types between StreamingExchangeSink and StreamingExchangeSource.
    enum PacketType
    {
        SinkHello   = 0x5104e110,
        SourceHello = 0x5004e110,
        Data        = 0x0000da7a,
    };
}
}
