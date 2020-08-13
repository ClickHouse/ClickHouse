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

uint64_t readLengthEncodedNumber(ReadBuffer & ss);
void readLengthEncodedString(String & s, ReadBuffer & buffer);

//inline void readLengthEncodedString(String & s, ReadBuffer & buffer)
//{
//    uint64_t len = readLengthEncodedNumber(buffer);
//    s.resize(len);
//    buffer.readStrict(reinterpret_cast<char *>(s.data()), len);
//}

}

}
