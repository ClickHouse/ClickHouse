#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <base/types.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

namespace RedisProtocol
{

/// Limits on the client-controlled length prefixes, mirroring the Redis protocol limits
/// (`PROTO_MAX_MULTIBULK_LEN` and the default `proto-max-bulk-len`).
/// Without them a client could make the server allocate an arbitrary amount of memory
/// by sending only a length header.
static constexpr Int64 MAX_ARRAY_SIZE = 1024 * 1024;
static constexpr Int64 MAX_BULK_STRING_SIZE = 64 * 1024 * 1024;

enum class DataType : char
{
    SIMPLE_STRING = '+',
    ERROR = '-',
    INTEGER = ':',
    BULK_STRING = '$',
    ARRAY = '*',
};

class Reader
{
public:
    explicit Reader(ReadBuffer & buf_) : buf(buf_) {}

    DataType readType()
    {
        char byte = 0;
        if (!buf.read(byte))
            throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Cannot read RESP data type");
        return static_cast<DataType>(byte);
    }

    Int64 readInteger()
    {
        Int64 num = 0;
        readIntText(num, buf);
        assertString("\r\n", buf);
        return num;
    }

    String readBulkString()
    {
        auto type = readType();
        if (type != DataType::BULK_STRING)
            throw Exception(
                ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Tried to read RESP type {} as a bulk string", static_cast<char>(type));

        auto size = readInteger();
        if (size < 0)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Negative bulk string length {}", size);
        if (size > MAX_BULK_STRING_SIZE)
            throw Exception(
                ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Bulk string length {} exceeds the maximum allowed {}", size, MAX_BULK_STRING_SIZE);

        String s;
        s.resize(size);
        buf.readStrict(s.data(), size);
        assertString("\r\n", buf);
        return s;
    }

private:
    ReadBuffer & buf;
};

class Writer
{
public:
    explicit Writer(WriteBuffer & buf_) : buf(buf_) {}

    void writeSimpleString(const String & s)
    {
        writeDataType(DataType::SIMPLE_STRING);
        writeString(s, buf);
        writeCRLF();
    }

    void writeError(const String & s)
    {
        writeDataType(DataType::ERROR);
        writeString("ERR ", buf);
        writeString(s, buf);
        writeCRLF();
    }

    void writeInteger(Int64 num)
    {
        writeDataType(DataType::INTEGER);
        writeIntText(num, buf);
        writeCRLF();
    }

    void writeBulkString(const String & s)
    {
        writeDataType(DataType::BULK_STRING);
        writeIntText(s.size(), buf);
        writeCRLF();
        writeString(s, buf);
        writeCRLF();
    }

    void writeArray(Int64 num)
    {
        writeDataType(DataType::ARRAY);
        writeIntText(num, buf);
        writeCRLF();
    }

    void writeNil()
    {
        writeDataType(DataType::BULK_STRING);
        writeString("-1", buf);
        writeCRLF();
    }

private:
    void writeDataType(DataType type)
    {
        buf.write(static_cast<char>(type));
    }

    void writeCRLF()
    {
        writeString("\r\n", buf);
    }

    WriteBuffer & buf;
};

}

}
