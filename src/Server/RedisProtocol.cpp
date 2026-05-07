#include <Server/RedisProtocol.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <cctype>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

namespace RedisProtocol
{

namespace
{

[[noreturn]] void throwProtocolError(std::string_view message)
{
    throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "Redis protocol error: {}", message);
}

char readByte(ReadBuffer & in)
{
    if (in.eof())
        throwProtocolError("unexpected end of stream");

    char value = 0;
    readChar(value, in);
    return value;
}

void readCRLF(ReadBuffer & in)
{
    if (readByte(in) != '\r' || readByte(in) != '\n')
        throwProtocolError("expected CRLF");
}

UInt64 readUnsignedLine(ReadBuffer & in)
{
    UInt64 value = 0;
    bool has_digit = false;

    while (true)
    {
        char c = readByte(in);
        if (c == '\r')
        {
            if (readByte(in) != '\n')
                throwProtocolError("expected LF after CR");
            if (!has_digit)
                throwProtocolError("expected number");
            return value;
        }

        if (c < '0' || c > '9')
            throwProtocolError("expected digit");

        has_digit = true;
        UInt64 digit = static_cast<UInt64>(c - '0');
        if (value > (std::numeric_limits<UInt64>::max() - digit) / 10)
            throwProtocolError("number is too large");
        value = value * 10 + digit;
    }
}

UInt64 readBulkStringSize(ReadBuffer & in)
{
    if (readByte(in) != '$')
        throwProtocolError("expected bulk string");

    return readUnsignedLine(in);
}

template <typename Size = size_t>
Size checkedSize(UInt64 size, std::string_view value_name)
{
    if constexpr (sizeof(UInt64) > sizeof(Size))
    {
        if (size > std::numeric_limits<Size>::max())
        {
            if (value_name == "array")
                throwProtocolError("array is too large");
            throwProtocolError("bulk string is too large");
        }
    }

    return static_cast<Size>(size);
}

String readBulkString(ReadBuffer & in)
{
    UInt64 size = readBulkStringSize(in);
    size_t string_size = checkedSize(size, "bulk string");

    String value;
    value.resize(string_size);

    for (char & c : value)
        c = readByte(in);

    readCRLF(in);
    return value;
}

void uppercaseASCII(String & value)
{
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c)
    {
        return static_cast<char>(std::toupper(c));
    });
}

}

Command readCommand(ReadBuffer & in)
{
    if (readByte(in) != '*')
        throwProtocolError("expected array");

    UInt64 size = readUnsignedLine(in);
    if (size == 0)
        throwProtocolError("expected non-empty command array");
    size_t array_size = checkedSize(size, "array");

    Command command;
    command.name = readBulkString(in);
    if (command.name.empty())
        throwProtocolError("expected command name");

    uppercaseASCII(command.name);

    command.arguments.reserve(array_size - 1);
    for (UInt64 i = 1; i < size; ++i)
        command.arguments.emplace_back(readBulkString(in));

    return command;
}

void writeSimpleString(WriteBuffer & out, std::string_view value)
{
    writeChar('+', out);
    writeString(value, out);
    writeCString("\r\n", out);
}

void writeError(WriteBuffer & out, std::string_view value)
{
    writeChar('-', out);
    writeString(value, out);
    writeCString("\r\n", out);
}

void writeBulkString(WriteBuffer & out, std::string_view value)
{
    writeChar('$', out);
    writeIntText(value.size(), out);
    writeCString("\r\n", out);
    writeString(value, out);
    writeCString("\r\n", out);
}

void writeNullBulkString(WriteBuffer & out)
{
    writeCString("$-1\r\n", out);
}

void writeArrayHeader(WriteBuffer & out, size_t size)
{
    writeChar('*', out);
    writeIntText(size, out);
    writeCString("\r\n", out);
}

}

}
