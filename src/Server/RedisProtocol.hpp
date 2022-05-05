#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <boost/pending/detail/property.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
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

    class Writer
    {
    public:
        explicit Writer(WriteBuffer * buf_) : buf(buf_) { }

        void writeBulkString(const String & s)
        {
            writeDataType(DataType::BULK_STRING);
            writeString(std::to_string(s.size()), *buf);
            writeCRLF();
            writeString(s, *buf);
            writeCRLF();
        }

        void writeErrorString(const String & s)
        {
            writeDataType(DataType::ERROR);
            writeString(s, *buf);
            writeCRLF();
        }

    private:
        void writeDataType(DataType type) { buf->write(static_cast<char>(type)); }

        void writeCRLF()
        {
            const char * crlf = "\r\n";
            buf->write(crlf, 2);
        }

        WriteBuffer * buf;
    };

    // *2\r\n $3\r\n GET\r\n $8\r\n some_key\r\n
    class GetRequest
    {
    public:
        void deserialize(ReadBuffer & in)
        {
            key.clear();
            Reader reader(&in);

            DataType type = reader.readType();
            if (type != DataType::ARRAY)
            {
                throw Exception(
                    Poco::format("Wrong RESP type. Type byte was %c.", static_cast<char>(type)), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            Int64 array_size = reader.readNumber();
            if (array_size != 2)
            {
                throw Exception(
                    Poco::format("Invalid array size. Array size was %d.", array_size), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            for (Int64 i = 0; i < array_size; ++i)
            {
                type = reader.readType();
                if (type != DataType::BULK_STRING)
                {
                    throw Exception(
                        Poco::format("Client sent wrong type. Type byte was %c.", static_cast<char>(type)),
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
                }
                String data = reader.readBulkString();
                if (i == 0) // Operation itself
                {
                    if (Poco::toLower(data) != "get")
                    {
                        throw Exception(Poco::format("Client sent unsupported method %s.", data), ErrorCodes::NOT_IMPLEMENTED);
                    }
                }
                else
                {
                    key = std::move(data);
                }
            }
        }

        const String & getKey() const { return key; }

    private:
        String key;
    };

    class GetResponse
    {
    public:
        explicit GetResponse(const String & value_) : value(value_) { }

        void serialize(WriteBuffer & out)
        {
            Writer writer(&out);
            writer.writeBulkString(value);
        }

    private:
        const String & value;
    };


    class ErrorResponse
    {
    public:
        explicit ErrorResponse(const String & error_) : error(error_) { }

        void serialize(WriteBuffer & out)
        {
            Writer writer(&out);
            writer.writeErrorString(error);
        }

    private:
        const String & error;
    };
}
}
