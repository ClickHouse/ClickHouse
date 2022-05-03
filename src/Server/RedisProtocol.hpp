#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_PACKET_FROM_CLIENT;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_TYPE;
}

namespace RedisProtocol
{
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
        explicit Reader(ReadBuffer * buf_) : buf(buf_) { }

        DataType readType()
        {
            char byte;
            buf->read(byte);
            return static_cast<DataType>(byte);
        }

        Int64 readNumber()
        {
            Int64 num;
            readIntTextImpl(num, *buf);
            skipToCRLFOrEOF(*buf);
            return num;
        }

        String readBulkString()
        {
            Int64 size = readNumber();
            String s;
            s.reserve(size);
            readCommand(s);
            return s;
        }

    private:
        void readCommand(String & s)
        {
            readStringUntilCarriageReturn(s, *buf);
            skipToCRLFOrEOF(*buf);
        }

        ReadBuffer * buf;
    };

    // *2\r\n $3\r\n GET\r\n $8\r\n some_key\r\n
    class GetRequest
    {
    public:
        void deserialize(ReadBuffer & in)
        {
            keys.clear();
            Reader reader(&in);

            DataType type = reader.readType();
            if (type != DataType::ARRAY)
            {
                throw Exception(
                    Poco::format("Client sent wrong message or closed the connection. Message byte was %c.", static_cast<char>(type)),
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            Int64 array_size = reader.readNumber();
            if (array_size < 2)
            {
                throw Exception(
                    Poco::format("Client sent wrong message. Number of words was %d.", array_size),
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }
            if (keys.capacity() < static_cast<size_t>(array_size - 1))
            {
                keys.reserve(array_size - 1);
            }

            // Reading redis operation
            if (reader.readType() != DataType::BULK_STRING)
            {
                throw Exception(
                    Poco::format("Client sent wrong message. Number of words was %d.", array_size),
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            for (Int64 i = 0; i < array_size; ++i)
            {
                if (reader.readType() != DataType::BULK_STRING)
                {
                    throw Exception(
                        Poco::format("Client sent wrong message. Number of words was %d.", array_size),
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
                }
                String data = reader.readBulkString();
                if (i == 0) // Operation itself
                {
                    if (data != "GET")
                    {
                        throw Exception(
                            Poco::format("Client sent wrong message. Number of words was %d.", array_size),
                            ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
                    }
                }
                else
                {
                    keys.push_back(std::move(data));
                }
            }
        }

        const std::vector<String> & getKeys() const { return keys; }

    private:
        std::vector<String> keys;
    };
}
}
