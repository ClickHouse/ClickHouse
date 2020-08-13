#include "MySQLProtocol.h"
#include <IO/WriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <common/logger_useful.h>

#include <random>
#include <sstream>


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

ColumnDefinitionPacket getColumnDefinition(const String & column_name, const TypeIndex type_index)
{
    ColumnType column_type;
    CharacterSet charset = CharacterSet::binary;
    int flags = 0;
    switch (type_index)
    {
        case TypeIndex::UInt8:
            column_type = ColumnType::MYSQL_TYPE_TINY;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt16:
            column_type = ColumnType::MYSQL_TYPE_SHORT;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt32:
            column_type = ColumnType::MYSQL_TYPE_LONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::UInt64:
            column_type = ColumnType::MYSQL_TYPE_LONGLONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG | ColumnDefinitionFlags::UNSIGNED_FLAG;
            break;
        case TypeIndex::Int8:
            column_type = ColumnType::MYSQL_TYPE_TINY;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int16:
            column_type = ColumnType::MYSQL_TYPE_SHORT;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int32:
            column_type = ColumnType::MYSQL_TYPE_LONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Int64:
            column_type = ColumnType::MYSQL_TYPE_LONGLONG;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Float32:
            column_type = ColumnType::MYSQL_TYPE_FLOAT;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Float64:
            column_type = ColumnType::MYSQL_TYPE_DOUBLE;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::Date:
            column_type = ColumnType::MYSQL_TYPE_DATE;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::DateTime:
            column_type = ColumnType::MYSQL_TYPE_DATETIME;
            flags = ColumnDefinitionFlags::BINARY_FLAG;
            break;
        case TypeIndex::String:
        case TypeIndex::FixedString:
            column_type = ColumnType::MYSQL_TYPE_STRING;
            charset = CharacterSet::utf8_general_ci;
            break;
        default:
            column_type = ColumnType::MYSQL_TYPE_STRING;
            charset = CharacterSet::utf8_general_ci;
            break;
    }
    return ColumnDefinitionPacket(column_name, charset, 0, column_type, flags, 0);
}

bool PacketPayloadReadBuffer::nextImpl()
{
    if (!has_read_header || (payload_length == max_packet_size && offset == payload_length))
    {
        has_read_header = true;
        working_buffer.resize(0);
        offset = 0;
        payload_length = 0;
        in.readStrict(reinterpret_cast<char *>(&payload_length), 3);

        if (payload_length > max_packet_size)
        {
            std::ostringstream tmp;
            tmp << "Received packet with payload larger than max_packet_size: " << payload_length;
            throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }

        size_t packet_sequence_id = 0;
        in.read(reinterpret_cast<char &>(packet_sequence_id));
        if (packet_sequence_id != sequence_id)
        {
            std::ostringstream tmp;
            tmp << "Received packet with wrong sequence-id: " << packet_sequence_id << ". Expected: " << static_cast<unsigned int>(sequence_id) << '.';
            throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
        }
        sequence_id++;

        if (payload_length == 0)
            return false;
    }
    else if (offset == payload_length)
    {
        return false;
    }

    in.nextIfAtEnd();
    working_buffer = ReadBuffer::Buffer(in.position(), in.buffer().end());
    size_t count = std::min(in.available(), payload_length - offset);
    working_buffer.resize(count);
    in.ignore(count);

    offset += count;

    return true;
}

PacketPayloadReadBuffer::PacketPayloadReadBuffer(ReadBuffer & in_, uint8_t & sequence_id_)
    : ReadBuffer(in_.position(), 0), in(in_), sequence_id(sequence_id_) // not in.buffer().begin(), because working buffer may include previous packet
{
}

//void ReadPacket::readPayload(ReadBuffer & in, uint8_t & sequence_id)
//{
//    PacketPayloadReadBuffer payload(in, sequence_id);
//    payload.next();
//    readPayloadImpl(payload);
//    if (!payload.eof())
//    {
//        std::stringstream tmp;
//        tmp << "Packet payload is not fully read. Stopped after " << payload.count() << " bytes, while " << payload.available() << " bytes are in buffer.";
//        throw ProtocolError(tmp.str(), ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT);
//    }
//}
//
//void LimitedReadPacket::readPayload(ReadBuffer & in, uint8_t & sequence_id)
//{
//    LimitReadBuffer limited(in, 10000, true, "too long MySQL packet.");
//    ReadPacket::readPayload(limited, sequence_id);
//}

}
