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
    extern const int NOT_IMPLEMENTED;
}

RedisHandler::RedisHandler(const Poco::Net::StreamSocket & socket_, IServer & server_, TCPServer & tcp_server_)
    : Poco::Net::TCPServerConnection(socket_), server(server_), tcp_server(tcp_server_)
{
    in = std::make_shared<ReadBufferFromPocoSocket>(socket());
    out = std::make_shared<WriteBufferFromPocoSocket>(socket());
    if (auto pass = server.config().getString("redis.requirepass", ""); !pass.empty())
    {
        if (auto user = server.config().getString("redis.username", ""); !user.empty())
        {
            username = std::move(user);
        }
        password = std::move(pass);
        authenticated = false;
    }
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
                "Can't use SSL for redis protocol, because ClickHouse was built without SSL library", DB::ErrorCodes::SUPPORT_IS_DISABLED);
#endif
        }

        while (tcp_server.isOpen())
        {
            SCOPE_EXIT(out->next());
            RedisProtocol::BeginRequest req;
            req.deserialize(*in);
            if (req.getMethod() == "get")
            {
                RedisProtocol::GetRequest get_req(req);
                get_req.deserialize(*in);
                log->log(Poco::format("GET request for %s key", get_req.getKey()));

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                RedisProtocol::BulkStringResponse resp("Hello world");
                resp.serialize(*out);
            }
            else if (req.getMethod() == "auth")
            {
                RedisProtocol::AuthRequest auth_req(req);
                auth_req.deserialize(*in);
                log->log(Poco::format("AUTH request for %s username", auth_req.getUsername()));

                if (auth_req.getUsername() != username || auth_req.getPassword() != password)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::WRONGPASS);
                    resp.serialize(*out);
                    continue;
                }

                authenticated = true;

                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "select")
            {
                RedisProtocol::SelectRequest select_req(req);
                select_req.deserialize(*in);
                log->log(Poco::format("SELECT request for %?d db", select_req.getDb()));

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                // TODO: Validate db value
                db = select_req.getDb();

                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "reset")
            {
                log->log(Poco::format("Reset %?d", 228));
                RedisProtocol::SimpleStringResponse resp("RESET");
                resp.serialize(*out);
            }
            else if (req.getMethod() == "quit")
            {
                log->log(Poco::format("Quit %?d", 228));
                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
                return;
            }
            else
            {
                throw Exception("ERR Unknown command", ErrorCodes::NOT_IMPLEMENTED);
            }
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
