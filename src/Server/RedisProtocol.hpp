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
    extern const int BAD_REQUEST_PARAMETER;
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

    namespace Message
    {
        const String OK = "OK";
        const String NOAUTH = "NOAUTH Authentication required.";
        const String WRONGPASS = "WRONGPASS invalid username-password pair or user is disabled.";
    }

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
            if (auto type = readType(); type != DataType::BULK_STRING)
            {
                throw Exception(
                    Poco::format("Client sent wrong type. Type byte was %c.", static_cast<char>(type)),
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }
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

        void writeSimpleString(const String & s)
        {
            writeDataType(DataType::SIMPLE_STRING);
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

    class Request
    {
    public:
        virtual void deserialize(ReadBuffer & in) = 0;

        virtual ~Request() = default;
    };

    class BeginRequest : public Request
    {
    public:
        virtual void deserialize(ReadBuffer & in) final
        {
            Reader reader(&in);

            DataType type = reader.readType();
            if (type != DataType::ARRAY)
            {
                throw Exception(
                    Poco::format("Wrong RESP type. Type byte was %c.", static_cast<char>(type)), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            array_size = reader.readNumber() - 1; // Minus method element
            if (array_size < 0)
            {
                throw Exception(Poco::format("Negative array size: %?d.", array_size), ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT);
            }

            method = Poco::toLower(reader.readBulkString());
        }

        Int64 getArraySize() const { return array_size; }

        const String & getMethod() const { return method; }

    private:
        Int64 array_size = 0;
        String method;
    };

    class AuthRequest : public Request
    {
    public:
        explicit AuthRequest(BeginRequest & req_) : req(req_) { }

        virtual void deserialize(ReadBuffer & in) final
        {
            Reader reader(&in);
            switch (auto array_size = req.getArraySize())
            {
                case 1:
                    password = reader.readBulkString();
                    break;
                case 2:
                    username = reader.readBulkString();
                    password = reader.readBulkString();
                    break;
                default:
                    throw Exception(Poco::format("Invalid array size. Array size was %?d.", array_size), ErrorCodes::BAD_REQUEST_PARAMETER);
            }
        }

        const String & getUsername() const { return username; }

        const String & getPassword() const { return password; }

    private:
        String username;
        String password;
        BeginRequest & req;
    };

    class SelectRequest : public Request
    {
    public:
        explicit SelectRequest(BeginRequest & req_) : req(req_) { }

        virtual void deserialize(ReadBuffer & in) final
        {
            Reader reader(&in);
            if (auto array_size = req.getArraySize(); array_size != 1)
            {
                throw Exception(Poco::format("Invalid array size. Array size was %?d.", array_size), ErrorCodes::BAD_REQUEST_PARAMETER);
            }
            db = stoi(reader.readBulkString());
        }

        Int64 getDb() const { return db; }

    private:
        Int64 db = 0;
        BeginRequest & req;
    };

    class GetRequest : public Request
    {
    public:
        explicit GetRequest(BeginRequest & req_) : req(req_) { }

        void deserialize(ReadBuffer & in) final
        {
            key.clear();
            Reader reader(&in);

            if (auto array_size = req.getArraySize(); array_size != 1)
            {
                throw Exception(Poco::format("Invalid array size. Array size was %?d.", array_size), ErrorCodes::BAD_REQUEST_PARAMETER);
            }

            key = reader.readBulkString();
        }

        const String & getKey() const { return key; }

    private:
        String key;
        BeginRequest & req;
    };

    class Response
    {
    public:
        virtual void serialize(WriteBuffer & out) = 0;

        virtual ~Response() = default;
    };

    class SimpleStringResponse : public Response
    {
    public:
        explicit SimpleStringResponse(const String & value_) : value(value_) { }

        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            writer.writeSimpleString(value);
        }

    private:
        const String & value;
    };

    class BulkStringResponse : public Response
    {
    public:
        explicit BulkStringResponse(const String & value_) : value(value_) { }

        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            writer.writeBulkString(value);
        }

    private:
        const String & value;
    };


    class ErrorResponse : public Response
    {
    public:
        explicit ErrorResponse(const String & error_) : error(error_) { }

        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            writer.writeErrorString(error);
        }

    private:
        const String & error;
    };
}
}
