#include <Core/MySQL/IMySQLReadPacket.h>
#include <Core/MySQLProtocol.h>

namespace DB
{

namespace MySQLProtocol
{

void IMySQLReadPacket::readPayload(ReadBuffer &in, uint8_t &sequence_id)
{
    PacketPayloadReadBuffer payload(in, sequence_id);
    payload.next();
    readPayloadImpl(payload);
    if (!payload.eof())
    {
        std::stringstream tmp;
        tmp << "Packet payload is not fully read. Stopped after " << payload.count() << " bytes, while " << payload.available() << " bytes are in buffer.";
        throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
    }
}

void LimitedReadPacket::readPayload(ReadBuffer &in, uint8_t &sequence_id)
{
    LimitReadBuffer limited(in, 10000, true, "too long MySQL packet.");
    ReadPacket::readPayload(limited, sequence_id);
}

}

}
