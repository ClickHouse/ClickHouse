#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <common/logger_useful.h>
#include <random>
#include <sstream>
#include "MySQLProtocol.h"


namespace DB
{
namespace MySQLProtocol
{

void PacketSender::resetSequenceId()
{
    sequence_id = 0;
}

String PacketSender::packetToText(String payload)
{
    String result;
    for (auto c : payload)
    {
        result += ' ';
        result += std::to_string(static_cast<unsigned char>(c));
    }
    return result;
}

uint64_t readLengthEncodedNumber(std::istringstream & ss)
{
    char c;
    uint64_t buf = 0;
    ss.get(c);
    auto cc = static_cast<uint8_t>(c);
    if (cc < 0xfc)
    {
        return cc;
    }
    else if (cc < 0xfd)
    {
        ss.read(reinterpret_cast<char *>(&buf), 2);
    }
    else if (cc < 0xfe)
    {
        ss.read(reinterpret_cast<char *>(&buf), 3);
    }
    else
    {
        ss.read(reinterpret_cast<char *>(&buf), 8);
    }
    return buf;
}

std::string writeLengthEncodedNumber(uint64_t x)
{
    std::string result;
    if (x < 251)
    {
        result.append(1, static_cast<char>(x));
    }
    else if (x < (1 << 16))
    {
        result.append(1, 0xfc);
        result.append(reinterpret_cast<char *>(&x), 2);
    }
    else if (x < (1 << 24))
    {
        result.append(1, 0xfd);
        result.append(reinterpret_cast<char *>(&x), 3);
    }
    else
    {
        result.append(1, 0xfe);
        result.append(reinterpret_cast<char *>(&x), 8);
    }
    return result;
}

void writeLengthEncodedString(std::string & payload, const std::string & s)
{
    payload.append(writeLengthEncodedNumber(s.length()));
    payload.append(s);
}

void writeNulTerminatedString(std::string & payload, const std::string & s)
{
    payload.append(s);
    payload.append(1, 0);
}

}
}
