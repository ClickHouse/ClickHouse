#pragma once

#include <boost/noncopyable.hpp>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include "IMySQLReadPacket.h"
#include "IMySQLWritePacket.h"
#include "IO/MySQLPacketPayloadReadBuffer.h"

namespace DB
{

namespace MySQLProtocol
{

/* Writes and reads packets, keeping sequence-id.
 * Throws ProtocolError, if packet with incorrect sequence-id was received.
 */
class PacketEndpoint : boost::noncopyable
{
public:
    uint8_t & sequence_id;
    ReadBuffer * in;
    WriteBuffer * out;

    /// For writing.
    PacketEndpoint(WriteBuffer & out_, uint8_t & sequence_id_);

    /// For reading and writing.
    PacketEndpoint(ReadBuffer & in_, WriteBuffer & out_, uint8_t & sequence_id_);

    MySQLPacketPayloadReadBuffer getPayload();

    void receivePacket(IMySQLReadPacket & packet);

    bool tryReceivePacket(IMySQLReadPacket & packet, UInt64 millisecond = 0);

    /// Sets sequence-id to 0. Must be called before each command phase.
    void resetSequenceId();

    template<class T>
    void sendPacket(const T & packet, bool flush = false)
    {
        static_assert(std::is_base_of<IMySQLWritePacket, T>());
        packet.writePayload(*out, sequence_id);
        if (flush)
            out->next();
    }

    /// Converts packet to text. Is used for debug output.
    static String packetToText(const String & payload);
};

using PacketEndpointPtr = std::shared_ptr<PacketEndpoint>;

}

}
