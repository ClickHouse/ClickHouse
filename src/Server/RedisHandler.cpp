#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include "RedisHandler.h"
#include "RedisProtocol.hpp"

#include <Server/TCPServer.h>
#include <Common/setThreadName.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYNTAX_ERROR;
}

RedisHandler::RedisHandler(
    const Poco::Net::StreamSocket & socket_,
    TCPServer & tcp_server_)
    : Poco::Net::TCPServerConnection(socket_)
    , tcp_server(tcp_server_)
{
    changeIO(socket());
}

void RedisHandler::changeIO(Poco::Net::StreamSocket & socket)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket);
    out = std::make_shared<WriteBufferFromPocoSocket>(socket);
}

void RedisHandler::run()
{
    setThreadName("RedisHandler");
    try
    {
        while (tcp_server.isOpen())
        {
            RedisProtocol::GetRequest req;
            req.deserialize(*in);
        }
    }
    catch (const Poco::Exception &exc)
    {
        log->log(exc);
    }

}

}
