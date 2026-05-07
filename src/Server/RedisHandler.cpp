#include <Server/RedisHandler.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Server/IServer.h>
#include <Server/RedisProtocol.h>
#include <Server/TCPServer.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <charconv>
#include <memory>
#include <system_error>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

RedisHandler::RedisHandler(
    IServer & server_,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    UInt64 connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , server(server_)
    , tcp_server(tcp_server_)
    , log(getLogger("RedisHandler"))
    , connection_id(connection_id_)
{
}

bool RedisHandler::selectDatabase(const String & db_index)
{
    if (db_index.empty())
        return false;

    UInt64 parsed_db = 0;
    const char * begin = db_index.data();
    const char * end = db_index.data() + db_index.size();
    auto result = std::from_chars(begin, end, parsed_db);
    if (result.ec != std::errc{} || result.ptr != end)
        return false;

    const auto & config = server.config();
    String prefix = "redis.db._" + std::to_string(parsed_db);

    TargetConfig target;
    target.database = config.getString(prefix + ".database", "");
    target.table = config.getString(prefix + ".table", "");
    target.default_column = config.getString(prefix + ".default_column", "");

    if (target.database.empty() || target.table.empty() || target.default_column.empty())
        return false;

    selected_db = parsed_db;
    selected_target = std::move(target);
    return true;
}

void RedisHandler::run()
{
    LOG_TRACE(log, "Redis connection started. Id: {}. Address: {}", connection_id, socket().peerAddress().toString());

    auto in = std::make_unique<ReadBufferFromPocoSocket>(socket());
    auto out = std::make_unique<AutoCanceledWriteBuffer<WriteBufferFromPocoSocket>>(socket());

    try
    {
        while (tcp_server.isOpen())
        {
            RedisProtocol::Command command = RedisProtocol::readCommand(*in);

            if (command.name == "PING")
            {
                if (command.arguments.empty())
                    RedisProtocol::writeSimpleString(*out, "PONG");
                else if (command.arguments.size() == 1)
                    RedisProtocol::writeBulkString(*out, command.arguments.front());
                else
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'ping' command");
            }
            else if (command.name == "QUIT")
            {
                RedisProtocol::writeSimpleString(*out, "OK");
                out->finalize();
                return;
            }
            else if (command.name == "SELECT")
            {
                if (command.arguments.size() != 1)
                {
                    RedisProtocol::writeError(*out, "ERR wrong number of arguments for 'select' command");
                }
                else if (command.arguments.front().empty() || command.arguments.front().front() < '0' || command.arguments.front().front() > '9')
                {
                    RedisProtocol::writeError(*out, "ERR invalid DB index");
                }
                else
                {
                    UInt64 parsed_db = 0;
                    const String & db_index = command.arguments.front();
                    auto result = std::from_chars(db_index.data(), db_index.data() + db_index.size(), parsed_db);
                    if (result.ec != std::errc{} || result.ptr != db_index.data() + db_index.size())
                    {
                        RedisProtocol::writeError(*out, "ERR invalid DB index");
                    }
                    else if (selectDatabase(db_index))
                    {
                        RedisProtocol::writeSimpleString(*out, "OK");
                    }
                    else
                    {
                        RedisProtocol::writeError(*out, "ERR Redis DB " + std::to_string(parsed_db) + " is not configured");
                    }
                }
            }
            else
            {
                RedisProtocol::writeError(*out, "ERR unknown command '" + command.name + "'");
            }

            out->next();
        }

        out->finalize();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED)
        {
            LOG_TRACE(log, "Redis protocol error. Id: {}. Error: {}", connection_id, e.message());
            try
            {
                RedisProtocol::writeError(*out, "ERR Protocol error");
                out->finalize();
            }
            catch (...)
            {
                tryLogCurrentException(log, "Failed to send Redis protocol error");
            }
            return;
        }

        LOG_TRACE(log, "Redis connection finished with exception. Id: {}. Error: {}", connection_id, e.message());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Redis connection finished with unexpected exception");
    }
}

}
