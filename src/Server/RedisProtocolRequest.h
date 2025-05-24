#pragma once

#include <Poco/String.h>

#include "Common/Exception.h"
#include "IO/ReadBuffer.h"
#include "RedisProtocolReaderWriter.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
        extern const int UNKNOWN_PACKET_FROM_CLIENT;
        extern const int UNEXPECTED_PACKET_FROM_CLIENT;
    }

    namespace RedisProtocol
    {
        namespace Message
        {
            const String OK = "OK";
            const String PONG = "PONG";
            const String NO_SUCH_DB = "ERR DB index is out of range";
        }

        namespace Command 
        {
            const String AUTH = "AUTH";
            const String ECHO = "ECHO";
            const String PING = "PING";
            const String QUIT = "QUIT";
            const String SELECT = "SELECT";
        }

        enum class CommandType 
        {
            AUTH,
            ECHO,
            PING,
            QUIT,
            SELECT,
        };

        inline String toString(CommandType cmd_type) {
            switch(cmd_type) {
                case CommandType::AUTH:
                    return Command::AUTH;
                case CommandType::ECHO:
                    return Command::ECHO;
                case CommandType::PING:
                    return Command::PING;
                case CommandType::QUIT:
                    return Command::QUIT;
                case CommandType::SELECT:
                    return Command::SELECT;
            }
            // unreachable
            return fmt::format("Unknown command type ({}).", static_cast<int>(cmd_type));
        }

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
                if (cmd == Command::AUTH)
                {
                    command = CommandType::AUTH;
                }
                else if (cmd == Command::ECHO)
                {
                    command = CommandType::ECHO;
                }
                else if (cmd == Command::PING)
                {
                    command = CommandType::PING;
                }
                else if (cmd == Command::QUIT)
                {
                    command = CommandType::QUIT;
                }
                else if (cmd == Command::SELECT)
                {
                    command = CommandType::SELECT;
                }
                else
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
                db = static_cast<UInt32>(stoul(reader.readBulkString()));
            }

            UInt32 getDB() const { return db; }
        
        private:
            RedisRequest & request;
            UInt32 db = 0;
        };

        class EchoRequest : IRequest
        {
        public:
            explicit EchoRequest(RedisRequest & req) : request(req) {}

            void deserialize(ReadBuffer & in) final
            {
                Reader reader(&in);
                if (request.getCommandLen() != 2)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                    "wrong number of arguments for 'echo' command"
                    );
                }
                input = reader.readBulkString();
            }

            String getCommandInput() const { return input; }

        private:
            RedisRequest & request;
            String input;
        };
    }
}
