#include <Core/MySQL/IMySQLWritePacket.h>
#include <Core/MySQLProtocol.h>

namespace DB
{

namespace MySQLProtocol
{

void IMySQLWritePacket::writePayload(WriteBuffer & buffer, uint8_t & sequence_id) const
{
    PacketPayloadWriteBuffer buf(buffer, getPayloadSize(), sequence_id);
    writePayloadImpl(buf);
    buf.next();
    if (buf.remainingPayloadSize())
    {
        std::stringstream ss;
        ss << "Incomplete payload. Written " << getPayloadSize() - buf.remainingPayloadSize() << " bytes, expected " << getPayloadSize() << " bytes.";
        throw Exception(ss.str(), 0);
    }
}

}

}
