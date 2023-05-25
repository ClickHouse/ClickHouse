#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

namespace MySQLProtocol
{

class IMySQLReadPacket
{
public:
    IMySQLReadPacket() = default;

    virtual ~IMySQLReadPacket() = default;

    IMySQLReadPacket(IMySQLReadPacket &&) = default;

    virtual void readPayload(ReadBuffer & in, uint8_t & sequence_id);

    virtual void readPayloadWithUnpacked(ReadBuffer & in);

protected:
    virtual void readPayloadImpl(ReadBuffer & buf) = 0;
};

class LimitedReadPacket : public IMySQLReadPacket
{
public:
    void readPayload(ReadBuffer & in, uint8_t & sequence_id) override;

    void readPayloadWithUnpacked(ReadBuffer & in) override;
};

uint64_t readLengthEncodedNumber(ReadBuffer & buffer);
void readLengthEncodedString(String & s, ReadBuffer & buffer);

}

}
