#include <Core/MySQL/IMySQLWritePacket.h>
#include <IO/MySQLPacketPayloadWriteBuffer.h>
#include <sstream>

namespace DB
{

namespace MySQLProtocol
{

void IMySQLWritePacket::writePayload(WriteBuffer & buffer, uint8_t & sequence_id) const
{
    MySQLPacketPayloadWriteBuffer buf(buffer, getPayloadSize(), sequence_id);
    writePayloadImpl(buf);
    buf.next();
    if (buf.remainingPayloadSize())
    {
        std::stringstream ss;
        ss << "Incomplete payload. Written " << getPayloadSize() - buf.remainingPayloadSize() << " bytes, expected " << getPayloadSize() << " bytes.";
        throw Exception(ss.str(), 0);
    }
}

size_t getLengthEncodedNumberSize(uint64_t x)
{
    if (x < 251)
    {
        return 1;
    }
    else if (x < (1 << 16))
    {
        return 3;
    }
    else if (x < (1 << 24))
    {
        return 4;
    }
    else
    {
        return 9;
    }
}

size_t getLengthEncodedStringSize(const String & s)
{
    return getLengthEncodedNumberSize(s.size()) + s.size();
}

void writeLengthEncodedNumber(uint64_t x, WriteBuffer & buffer)
{
    if (x < 251)
    {
        buffer.write(static_cast<char>(x));
    }
    else if (x < (1 << 16))
    {
        buffer.write(0xfc);
        buffer.write(reinterpret_cast<char *>(&x), 2);
    }
    else if (x < (1 << 24))
    {
        buffer.write(0xfd);
        buffer.write(reinterpret_cast<char *>(&x), 3);
    }
    else
    {
        buffer.write(0xfe);
        buffer.write(reinterpret_cast<char *>(&x), 8);
    }
}

void writeLengthEncodedString(const String & s, WriteBuffer & buffer)
{
    writeLengthEncodedNumber(s.size(), buffer);
    buffer.write(s.data(), s.size());
}

void writeNulTerminatedString(const String & s, WriteBuffer & buffer)
{
    buffer.write(s.data(), s.size());
    buffer.write(0);
}

}

}
