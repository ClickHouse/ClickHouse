#include <Common/Exception.h>
#include <Core/MySQL/IMySQLReadPacket.h>
#include <IO/MySQLPacketPayloadReadBuffer.h>
#include <IO/LimitReadBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace MySQLProtocol
{

void IMySQLReadPacket::readPayload(ReadBuffer & in, uint8_t & sequence_id)
{
    MySQLPacketPayloadReadBuffer payload(in, sequence_id);
    payload.next();
    readPayloadImpl(payload);
    if (!payload.eof())
    {
        throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                        "Packet payload is not fully read. Stopped after {} bytes, while {} bytes are in buffer.",
                        payload.count(), payload.available());
    }
}

void IMySQLReadPacket::readPayloadWithUnpacked(ReadBuffer & in)
{
    readPayloadImpl(in);
}

void LimitedReadPacket::readPayload(ReadBuffer &in, uint8_t &sequence_id)
{
    LimitReadBuffer limited(in, {.read_no_more = 10000, .expect_eof = true, .excetion_hint = "too long MySQL packet."});
    IMySQLReadPacket::readPayload(limited, sequence_id);
}

void LimitedReadPacket::readPayloadWithUnpacked(ReadBuffer & in)
{
    LimitReadBuffer limited(in,{.read_no_more = 10000, .expect_eof = true, .excetion_hint = "too long MySQL packet."});
    IMySQLReadPacket::readPayloadWithUnpacked(limited);
}

uint64_t readLengthEncodedNumber(ReadBuffer & buffer, UInt16 & bytes_read)
{
    char c{};
    uint64_t buf = 0;
    buffer.readStrict(c);
    bytes_read = 1;
    auto cc = static_cast<uint8_t>(c);
    switch (cc)
    {
        /// NULL
        case 0xfb:
            break;
        case 0xfc:
            buffer.readStrict(reinterpret_cast<char *>(&buf), 2);
            bytes_read += 2;
            break;
        case 0xfd:
            buffer.readStrict(reinterpret_cast<char *>(&buf), 3);
            bytes_read += 3;
            break;
        case 0xfe:
            buffer.readStrict(reinterpret_cast<char *>(&buf), 8);
            bytes_read += 8;
            break;
        default:
            return cc;
    }
    return buf;
}

uint64_t readLengthEncodedNumber(ReadBuffer & buffer)
{
    UInt16 bytes_read = 0;
    return readLengthEncodedNumber(buffer, bytes_read);
}

void readLengthEncodedString(String & s, ReadBuffer & buffer)
{
    uint64_t len = readLengthEncodedNumber(buffer);
    /// `len` is fully attacker-controlled (up to 2^64-1 via the 0xfe prefix). Grow the string in
    /// bounded chunks as bytes actually arrive instead of resizing to `len` up front, so a bogus
    /// length cannot trigger a huge pre-emptive allocation (pre-auth on the MySQL handshake path).
    s.clear();
    while (s.size() < len)
    {
        if (buffer.eof())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                "Cannot read all data for a length-encoded string. Expected: {}, read: {}", len, s.size());
        size_t chunk = std::min(static_cast<uint64_t>(buffer.available()), len - s.size());
        size_t old_size = s.size();
        s.resize(old_size + chunk);
        buffer.readStrict(s.data() + old_size, chunk);
    }
}

}

}
