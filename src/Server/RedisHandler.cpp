#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include "RedisHandler.h"
#include "RedisProtocol.hpp"

#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>
#include <Common/setThreadName.h>

namespace DB
{

RedisHandler::RedisHandler(const Poco::Net::StreamSocket & socket_, TCPServer & tcp_server_)
    : Poco::Net::TCPServerConnection(socket_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
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
            log->log(Poco::format("GET request for %s key", req.getKey()));

            RedisProtocol::GetResponse resp("Hello world");
            resp.serialize(*out);
            out->next();
        }
    }
    catch (const Poco::Exception & exc)
    {
        log->log(exc);
        RedisProtocol::ErrorResponse resp(exc.message());
        resp.serialize(*out);
    }
}
}
