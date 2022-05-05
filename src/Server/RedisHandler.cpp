#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include "RedisHandler.h"
#include "RedisProtocol.hpp"

#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/setThreadName.h>

#if USE_SSL
#    include <Poco/Net/SSLManager.h>
#    include <Poco/Net/SecureStreamSocket.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

RedisHandler::RedisHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
}

void RedisHandler::run()
{
    setThreadName("RedisHandler");
    try
    {
        if (server.config().getBool("redis.enable_ssl", false))
        {
#if USE_SSL
            makeSecureConnection();
#else
            throw DB::Exception(
                "Can't use SSL in redis, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }

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

void RedisHandler::makeSecureConnection()
{
#if USE_SSL
    ss = std::make_shared<Poco::Net::SecureStreamSocket>(
        Poco::Net::SecureStreamSocket::attach(socket(), Poco::Net::SSLManager::instance().defaultServerContext()));
    in = std::make_shared<ReadBufferFromPocoSocket>(*ss);
    out = std::make_shared<WriteBufferFromPocoSocket>(*ss);
#endif
}
}
