#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <common/logger_useful.h>
#include <random>
#include <sstream>
#include "MySQLProtocol.h"


namespace DB::MySQLProtocol
{

void PacketSender::resetSequenceId()
{
    sequence_id = 0;
}

String PacketSender::packetToText(const String & payload)
{
    String result;
    for (auto c : payload)
    {
        result += ' ';
        result += std::to_string(static_cast<unsigned char>(c));
    }
    return result;
}

uint64_t readLengthEncodedNumber(ReadBuffer & ss)
{
    char c{};
    uint64_t buf = 0;
    ss.readStrict(c);
    auto cc = static_cast<uint8_t>(c);
    if (cc < 0xfc)
    {
        return cc;
    }
    else if (cc < 0xfd)
    {
        ss.readStrict(reinterpret_cast<char *>(&buf), 2);
    }
    else if (cc < 0xfe)
    {
        ss.readStrict(reinterpret_cast<char *>(&buf), 3);
    }
    else
    {
        ss.readStrict(reinterpret_cast<char *>(&buf), 8);
    }
    return buf;
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

}
