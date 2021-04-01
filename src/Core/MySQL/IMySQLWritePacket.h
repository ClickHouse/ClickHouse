#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{

namespace MySQLProtocol
{

class IMySQLWritePacket
{
public:
    IMySQLWritePacket() = default;

    virtual ~IMySQLWritePacket() = default;

    IMySQLWritePacket(IMySQLWritePacket &&) = default;

    virtual void writePayload(WriteBuffer & buffer, uint8_t & sequence_id) const;

protected:
    virtual size_t getPayloadSize() const = 0;

    virtual void writePayloadImpl(WriteBuffer & buffer) const = 0;
};

size_t getLengthEncodedNumberSize(uint64_t x);
size_t getLengthEncodedStringSize(const String & s);

void writeLengthEncodedNumber(uint64_t x, WriteBuffer & buffer);
void writeLengthEncodedString(const String & s, WriteBuffer & buffer);
void writeNulTerminatedString(const String & s, WriteBuffer & buffer);

}

}
