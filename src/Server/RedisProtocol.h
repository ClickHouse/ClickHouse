#pragma once

#include <string>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Session.h>
#include <Poco/String.h>
#include "Common/Exception.h"
#include <Common/logger_useful.h>

namespace DB{

    namespace ErrorCodes {
        extern const int BAD_ARGUMENTS;
        extern const int UNKNOWN_PACKET_FROM_CLIENT;
        extern const int UNEXPECTED_PACKET_FROM_CLIENT;
        extern const int NOT_IMPLEMENTED;
        extern const int UNKNOWN_TYPE;
        extern const int LIMIT_EXCEEDED;
    }

    namespace RedisProtocol {
        namespace Command 
        {
            const String PING = "PING";
            const String SELECT = "SELECT";
        }

        enum class CommandType 
        {
            PING,
            SELECT
        };

        String toString(CommandType cmd_type) {
            switch(cmd_type) {
                case CommandType::PING:
                    return Command::PING;
                case CommandType::SELECT:
                    return Command::SELECT;
            }
            return fmt::format("Unknown command type ({}).", static_cast<int>(cmd_type));
        }

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
            const String PONG = "PONG";
        }

        class Reader
        {
        public:
            explicit Reader(ReadBuffer* buf_) : buf(buf_) {}

            DataType readType() {
                char byte;
                bool success = buf->read(byte);
                if (!success) {
                    throw Exception(
                        ErrorCodes::CANNOT_PARSE_BOOL,
                        "Can't read redis struct type"
                    );
                }
                return static_cast<DataType>(byte);
            }

            Int64 readInteger() {
                Int64 num;
                readIntTextImpl(num, *buf);
                skipToCRLFOrEOF(*buf);
                return num;
            }

            String readBulkString() {
                auto type = readType();
                if (type != DataType::BULK_STRING) {
                    throw Exception(
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                        "Tried to read {} type as Bulk string", type
                    );
                }

                auto size = readInteger();
                String s;
                s.reserve(size);
                readStringUntilCR(s, *buf);
                skipToCRLFOrEOF(*buf);
                return s;
            }
        private:
            ReadBuffer* buf;
        };

        class Writer
        {
        public:
            explicit Writer(WriteBuffer * buf_) 
                : buf(buf_)
            {}

            void writeSimpleString(const String & s) 
            {
                writeDataType(DataType::SIMPLE_STRING);
                writeString(s, *buf);
                writeCRLF();
            }

            void writeError(const String & s) 
            {
                writeDataType(DataType::ERROR);
                writeString(s, *buf);
                writeCRLF();
            }

            void writeInteger(Int64 num)
            {
                writeDataType(DataType::INTEGER);
                writeString(std::to_string(num), *buf);
                writeCRLF();
            }

            void writeBulkString(const String & s)
            {
                writeDataType(DataType::BULK_STRING);
                writeString(std::to_string(s.size()), *buf);
                writeCRLF();
                writeString(s, *buf);
                writeCRLF();
            }

            void writeArray(Int64 num)
            {
                writeDataType(DataType::ARRAY);
                writeString(std::to_string(num), *buf);
                writeCRLF();
            }

        private:
            void writeDataType(DataType type) {
                buf->write(static_cast<char>(type));
            }

            void writeCRLF() {
                const char * crlf = "\r\n";
                buf->write(crlf, 2);
            }

            WriteBuffer * buf;
        };

        class IRequest
        {
        public:

            virtual void deserialize(ReadBuffer & in) = 0;

            virtual ~IRequest() = default;
        };

        class RedisRequest : IRequest
        {
        public:
            void deserialize(ReadBuffer & in) override
            {
                Reader reader(&in);
                DataType type = reader.readType();
                if (type != DataType::ARRAY) 
                {
                    throw Exception(
                        ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                        "Can't parse incomming request. Unexpected RESP type: {}", static_cast<char>(type)
                    );
                }

                command_len = reader.readInteger();
                if (command_len < 1) {
                    throw Exception(
                        ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                        "Wrong command length: {}", command_len
                    );
                }

                auto cmd = Poco::toUpper(reader.readBulkString());
                if (cmd == Command::PING) 
                {
                    command = CommandType::PING;
                } else
                {
                    throw Exception(
                        ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                        "Unknown command: {}", cmd
                    );
                }
            }

            Int64 getCommandLen() const { return command_len; }
            
            CommandType getCommand() const { return command; }

        private:
            CommandType command;
            Int64 command_len = 0;
        };

        class SelectRequest : IRequest
        {
        public:
            explicit SelectRequest(RedisRequest & req) : request(req) {}

            void deserialize(ReadBuffer & in) final
            {
                Reader reader(&in);
                if (request.getCommandLen() != 2)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "wrong number of arguments for 'select' command"  
                    );
                }
                db = stoi(reader.readBulkString());
            }

            Int64 getDB() const { return db; }
        
        private:
            RedisRequest & request;
            Int64 db = 0;
        };

        class IResponse 
        {
        public:
            virtual void serialize(WriteBuffer & out) = 0;
            
            virtual ~IResponse() = default;
        };

        class ErrorResponse : public IResponse
        {
        public:
            explicit ErrorResponse(const String & err) : error(err) {}

            void serialize(WriteBuffer & out) final
            {
                Writer writer(&out);
                writer.writeError(error);
            }
        private:
            const String & error;
        };
    }
}
