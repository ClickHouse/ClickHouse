#include <Server/RedisHandler.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Server/RedisProtocol.h>
#include <Server/TCPServer.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

RedisHandler::RedisHandler(
    IServer &,
    TCPServer & tcp_server_,
    const Poco::Net::StreamSocket & socket_,
    UInt64 connection_id_)
    : Poco::Net::TCPServerConnection(socket_)
    , tcp_server(tcp_server_)
    , log(getLogger("RedisHandler"))
    , connection_id(connection_id_)
{
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
