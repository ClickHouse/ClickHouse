#include <Core/MySQL/PacketEndpoint.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MySQLProtocol
{

PacketEndpoint::PacketEndpoint(WriteBuffer & out_, uint8_t & sequence_id_)
    : sequence_id(sequence_id_), in(nullptr), out(&out_)
{
}

PacketEndpoint::PacketEndpoint(ReadBuffer & in_, WriteBuffer & out_, uint8_t & sequence_id_)
    : sequence_id(sequence_id_), in(&in_), out(&out_)
{
}

MySQLPacketPayloadReadBuffer PacketEndpoint::getPayload()
{
    return MySQLPacketPayloadReadBuffer(*in, sequence_id);
}

void PacketEndpoint::receivePacket(IMySQLReadPacket & packet)
{
    packet.readPayload(*in, sequence_id);
}

bool PacketEndpoint::tryReceivePacket(IMySQLReadPacket & packet, UInt64 millisecond)
{
    if (millisecond != 0)
    {
        ReadBufferFromPocoSocket * socket_in = typeid_cast<ReadBufferFromPocoSocket *>(in);

        if (!socket_in)
            throw Exception("LOGICAL ERROR: Attempt to pull the duration in a non socket stream", ErrorCodes::LOGICAL_ERROR);

        if (!socket_in->poll(millisecond * 1000))
            return false;
    }

    packet.readPayload(*in, sequence_id);
    return true;
}

void PacketEndpoint::resetSequenceId()
{
    sequence_id = 0;
}

String PacketEndpoint::packetToText(const String & payload)
{
    String result;
    for (auto c : payload)
    {
        result += ' ';
        result += std::to_string(static_cast<unsigned char>(c));
    }
    return result;
}

}

}
