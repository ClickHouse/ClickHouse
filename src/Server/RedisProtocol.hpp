#pragma once

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <Common/logger_useful.h>
#include "Access/Common/AuthenticationData.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_REQUEST_PARAMETER;
    extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    extern const int NOT_IMPLEMENTED;
    extern const int AUTHENTICATION_FAILED;
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
        const String WRONGAUTH = "WRONGAUTH Not supported authentication method.";
        const String UNKNOWNCOMMAND = "ERR Unknown command";
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

        void writeArray(const Int64 size)
        {
            writeDataType(DataType::ARRAY);
            writeString(std::to_string(size), *buf);
            writeCRLF();
        }

        void writeNumber(const Int64 value)
        {
            writeDataType(DataType::INTEGER);
            writeString(std::to_string(value), *buf);
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

    class MGetRequest : public Request
    {
    public:
        explicit MGetRequest(BeginRequest & req_) : req(req_) { }

        void deserialize(ReadBuffer & in) final
        {
            keys.clear();
            Reader reader(&in);

            auto array_size = req.getArraySize();
            if (!array_size)
            {
                throw Exception(Poco::format("Invalid array size. Array size was %?d.", array_size), ErrorCodes::BAD_REQUEST_PARAMETER);
            }
            keys.resize(array_size);

            for (Int64 i = 0; i < array_size; ++i)
            {
                keys[i] = reader.readBulkString();
            }
        }

        const std::vector<String> & getKeys() const { return keys; }

    private:
        std::vector<String> keys;
        BeginRequest & req;
    };

    class HMGetRequest : public Request
    {
    public:
        explicit HMGetRequest(BeginRequest & req_) : req(req_) { }

        void deserialize(ReadBuffer & in) final
        {
            key.clear();
            columns.clear();

            Reader reader(&in);

            auto array_size = req.getArraySize();
            if (array_size < 2)
            {
                throw Exception(Poco::format("Invalid array size. Array size was %?d.", array_size), ErrorCodes::BAD_REQUEST_PARAMETER);
            }
            columns.resize(array_size - 1);

            key = reader.readBulkString();
            for (Int64 i = 0; i < array_size - 1; ++i)
            {
                columns[i] = reader.readBulkString();
            }
        }

        const String & getKey() const { return key; }

        const std::vector<String> & getColumns() const { return columns; }

    private:
        String key;
        std::vector<String> columns;
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

    class NilResponse : public Response
    {
    public:
        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            writer.writeNumber(-1);
        }
    };

    class BulkStringResponse : public Response
    {
    public:
        explicit BulkStringResponse(const std::optional<String> & value_) : value(value_) { }

        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            if (value.has_value())
            {
                writer.writeBulkString(value.value());
            }
            else
            {
                NilResponse resp;
                resp.serialize(out);
            }
        }

    private:
        const std::optional<String> & value;
    };

    class ArrayResponse : public Response
    {
    public:
        explicit ArrayResponse(const std::vector<std::optional<String>> & values_) : values(values_) { }

        void serialize(WriteBuffer & out) final
        {
            Writer writer(&out);
            writer.writeArray(values.size());
            for (const auto & value : values)
            {
                BulkStringResponse bulk_string(value);
                bulk_string.serialize(out);
            }
        }

    private:
        const std::vector<std::optional<String>> & values;
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

    class AuthenticationManager
    {
    public:
        bool authenticate(
            const String & username,
            const String & password,
            Session & session,
            const Poco::Net::SocketAddress & address,
            WriteBuffer * buf)
        {
            Writer writer(buf);
            const AuthenticationType user_auth_type = session.getAuthenticationTypeOrLogInFailure(username);
            if (user_auth_type != DB::AuthenticationType::PLAINTEXT_PASSWORD)
            {
                writer.writeErrorString(Message::WRONGAUTH);
                return false;
            }
            try
            {
                session.authenticate(username, password, address);
            }
            catch (const Exception & e)
            {
                if (e.code() == ErrorCodes::AUTHENTICATION_FAILED)
                    writer.writeErrorString(Message::WRONGPASS);
                else
                    writer.writeErrorString(Poco::format("AUTHERR %s", e.message()));
                return false;
            }
            writer.writeSimpleString(Message::OK);
            LOG_DEBUG(log, "Authentication for user {} was successful.", username);
            return true;
        }

    private:
        Poco::Logger * log = &Poco::Logger::get("RedisAuthenticationManager");
    };
}
}
