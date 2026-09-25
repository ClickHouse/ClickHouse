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
    /// Named `ECHO_COMMAND` rather than `ECHO`, because `<termios.h>` defines an `ECHO` macro on some platforms (e.g. ppc64le).
    const String ECHO_COMMAND = "ECHO";
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
    ECHO_COMMAND,
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
        case CommandType::ECHO_COMMAND:
            return Command::ECHO_COMMAND;
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
    /// Interprets the arguments of an already read command.
    virtual void parse() = 0;

    virtual ~IRequest() = default;
};

/// Reads a whole command: the RESP array header, the command name and all its arguments.
///
/// The command is always consumed completely before it is interpreted by `parse`, so that an error
/// response for an unknown command or a wrong number of arguments does not leave unread arguments
/// in the socket: the next request would then start in the middle of the previous command and
/// desynchronize the connection.
class RedisRequest : public IRequest
{
public:
    void deserialize(ReadBuffer & in)
    {
        Reader reader(in);
        DataType type = reader.readType();
        if (type != DataType::ARRAY)
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_CLIENT,
                "Cannot parse incoming request. Unexpected RESP type: {}", static_cast<char>(type));

        Int64 command_len = reader.readInteger();
        if (command_len < 1)
            throw Exception(ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT, "Wrong command length: {}", command_len);
        if (command_len > MAX_COMMAND_ELEMENTS)
            throw Exception(
                ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                "Command length {} exceeds the maximum allowed {}", command_len, MAX_COMMAND_ELEMENTS);

        arguments.clear();
        arguments.reserve(command_len);
        /// Every element is charged its payload plus a fixed overhead, because an element is held
        /// several times over (as the buffered argument, as a `Field` and inside the containers of
        /// the lookup), and for short arguments that overhead is what dominates the memory usage.
        size_t total_size = 0;
        for (Int64 i = 0; i < command_len; ++i)
        {
            arguments.push_back(reader.readBulkString());
            total_size += arguments.back().size() + COMMAND_ELEMENT_MEMORY_OVERHEAD;
            if (total_size > MAX_COMMAND_SIZE)
                throw Exception(
                    ErrorCodes::UNEXPECTED_PACKET_FROM_CLIENT,
                    "Total size of the command arguments exceeds the maximum allowed {}", MAX_COMMAND_SIZE);
        }
    }

    void parse() override
    {
        auto cmd = Poco::toUpper(arguments.front());
        if (cmd == Command::COMMAND)
            command = CommandType::COMMAND;
        else if (cmd == Command::AUTH)
            command = CommandType::AUTH;
        else if (cmd == Command::ECHO_COMMAND)
            command = CommandType::ECHO_COMMAND;
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

    /// The number of elements of the command array, including the command name itself.
    Int64 getCommandLen() const { return static_cast<Int64>(arguments.size()); }

    CommandType getCommand() const { return command; }

    /// Argument 0 is the command name.
    const String & getArgument(size_t index) const { return arguments[index]; }

private:
    CommandType command = CommandType::COMMAND;
    std::vector<String> arguments;
};

class SelectRequest : public IRequest
{
public:
    explicit SelectRequest(RedisRequest & req) : request(req) {}

    void parse() final
    {
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'select' command");
        db = DB::parse<UInt32>(request.getArgument(1));
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

    void parse() final
    {
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'echo' command");
        input = request.getArgument(1);
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

    void parse() final
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

    void parse() final
    {
        if (request.getCommandLen() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'get' command");
        key = request.getArgument(1);
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

    void parse() final
    {
        if (request.getCommandLen() < 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'mget' command");
    }

    /// The keys are not copied out of the request: for a command with many keys the copy alone
    /// would double the memory occupied by the command.
    size_t getKeysCount() const { return static_cast<size_t>(request.getCommandLen()) - 1; }

    const String & getKey(size_t index) const { return request.getArgument(index + 1); }

private:
    RedisRequest & request;
};

class HGetRequest : public IRequest
{
public:
    explicit HGetRequest(RedisRequest & req) : request(req) {}

    void parse() final
    {
        if (request.getCommandLen() != 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'hget' command");
        key = request.getArgument(1);
        field = request.getArgument(2);
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

    void parse() final
    {
        if (request.getCommandLen() < 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "wrong number of arguments for 'hmget' command");
        key = request.getArgument(1);
        fields.reserve(request.getCommandLen() - 2);
        for (Int64 i = 2; i < request.getCommandLen(); ++i)
            fields.push_back(request.getArgument(i));
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

    void parse() final
    {
        if (request.getCommandLen() == 2)
        {
            password = request.getArgument(1);
        }
        else if (request.getCommandLen() == 3)
        {
            user = request.getArgument(1);
            password = request.getArgument(2);
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

}

}
