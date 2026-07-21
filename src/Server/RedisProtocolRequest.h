#pragma once

#include <vector>

#include <Poco/String.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Server/RedisProtocolReaderWriter.h>
#include <Common/Exception.h>

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
    const String NO_SUCH_DB = "DB index is out of range";
}

namespace Command
{
    const String COMMAND = "COMMAND";
    const String CLIENT = "CLIENT";
    const String AUTH = "AUTH";
    const String ECHO = "ECHO";
    const String PING = "PING";
    const String QUIT = "QUIT";
    const String SELECT = "SELECT";

    const String GET = "GET";
    const String MGET = "MGET";

    const String HGET = "HGET";
    const String HMGET = "HMGET";
}

enum class CommandType : uint8_t
{
    COMMAND,
    CLIENT,
    AUTH,
    ECHO,
    PING,
    QUIT,
    SELECT,

    GET,
    MGET,

    HGET,
    HMGET,
};

inline String toString(CommandType cmd_type)
{
    switch (cmd_type)
    {
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
        case CommandType::GET:
            return Command::GET;
        case CommandType::MGET:
            return Command::MGET;
        case CommandType::HGET:
            return Command::HGET;
        case CommandType::HMGET:
            return Command::HMGET;
        case CommandType::COMMAND:
            return Command::COMMAND;
        case CommandType::CLIENT:
            return Command::CLIENT;
    }
}

class IRequest
{
public:
    virtual void deserialize(ReadBuffer & in) = 0;

    virtual ~IRequest() = default;
};

/// Reads the command array header and the command name, leaving the arguments in the buffer.
class RedisRequest : public IRequest
{
public:
    void deserialize(ReadBuffer & in) override
    {
        Reader reader(in);
        DataType type = reader.readType();
        if (type != DataType::ARRAY)
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Cannot parse incoming request. Unexpected RESP type: {}", static_cast<char>(type));

        command_len = reader.readInteger();
        if (command_len < 1)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Wrong command length: {}", command_len);

        auto cmd = Poco::toUpper(reader.readBulkString());
        if (cmd == Command::COMMAND)
            command = CommandType::COMMAND;
        else if (cmd == Command::AUTH)
            command = CommandType::AUTH;
        else if (cmd == Command::ECHO)
            command = CommandType::ECHO;
        else if (cmd == Command::PING)
            command = CommandType::PING;
        else if (cmd == Command::QUIT)
            command = CommandType::QUIT;
        else if (cmd == Command::SELECT)
            command = CommandType::SELECT;
        else if (cmd == Command::GET)
            command = CommandType::GET;
        else if (cmd == Command::MGET)
            command = CommandType::MGET;
        else if (cmd == Command::HGET)
            command = CommandType::HGET;
        else if (cmd == Command::HMGET)
            command = CommandType::HMGET;
        else if (cmd == Command::CLIENT)
            command = CommandType::CLIENT;
        else
            throw Exception(ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT, "Unknown command: {}", cmd);
    }

    Int64 getCommandLen() const { return command_len; }

    CommandType getCommand() const { return command; }

private:
    CommandType command = CommandType::COMMAND;
    Int64 command_len = 0;
};

class SelectRequest : public IRequest
{
public:
    explicit SelectRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'select' command");
        db = parse<UInt32>(reader.readBulkString());
    }

    UInt32 getDB() const { return db; }

private:
    RedisRequest & request;
    UInt32 db = 0;
};

class EchoRequest : public IRequest
{
public:
    explicit EchoRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'echo' command");
        input = reader.readBulkString();
    }

    const String & getCommandInput() const { return input; }

private:
    RedisRequest & request;
    String input;
};

class PingRequest : public IRequest
{
public:
    explicit PingRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer &) final
    {
        if (request.getCommandLen() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'ping' command");
    }

private:
    RedisRequest & request;
};

class GetRequest : public IRequest
{
public:
    explicit GetRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'get' command");
        key = reader.readBulkString();
    }

    const String & getKey() const { return key; }

private:
    RedisRequest & request;
    String key;
};

class MGetRequest : public IRequest
{
public:
    explicit MGetRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'mget' command");
        keys.reserve(request.getCommandLen() - 1);
        for (Int64 i = 1; i < request.getCommandLen(); ++i)
            keys.push_back(reader.readBulkString());
    }

    const std::vector<String> & getKeys() const { return keys; }

private:
    RedisRequest & request;
    std::vector<String> keys;
};

class HGetRequest : public IRequest
{
public:
    explicit HGetRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() != 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'hget' command");
        key = reader.readBulkString();
        field = reader.readBulkString();
    }

    const String & getKey() const { return key; }

    const String & getField() const { return field; }

private:
    RedisRequest & request;
    String key;
    String field;
};

class HMGetRequest : public IRequest
{
public:
    explicit HMGetRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() < 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'hmget' command");
        key = reader.readBulkString();
        fields.reserve(request.getCommandLen() - 2);
        for (Int64 i = 2; i < request.getCommandLen(); ++i)
            fields.push_back(reader.readBulkString());
    }

    const String & getKey() const { return key; }

    const std::vector<String> & getFields() const { return fields; }

private:
    RedisRequest & request;
    String key;
    std::vector<String> fields;
};

/// AUTH password | AUTH username password
class AuthRequest : public IRequest
{
public:
    explicit AuthRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        if (request.getCommandLen() == 2)
        {
            password = reader.readBulkString();
        }
        else if (request.getCommandLen() == 3)
        {
            user = reader.readBulkString();
            password = reader.readBulkString();
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'auth' command");
    }

    const String & getUser() const { return user; }

    const String & getPassword() const { return password; }

private:
    RedisRequest & request;
    String user = "default";
    String password;
};

/// Reads and discards all arguments of a command.
class CommandRequest : public IRequest
{
public:
    explicit CommandRequest(RedisRequest & req) : request(req) {}

    void deserialize(ReadBuffer & in) final
    {
        Reader reader(in);
        for (Int64 i = 1; i < request.getCommandLen(); ++i)
            reader.readBulkString();
    }

private:
    RedisRequest & request;
};

}

}
