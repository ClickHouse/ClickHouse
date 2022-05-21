#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>

#include "Core/iostream_debug_helpers.h"
#include "RedisHandler.h"
#include "RedisProtocol.hpp"

#include <Server/TCPServer.h>
#include <base/scope_guard.h>
#include <Poco/Exception.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/setThreadName.h>

#include <DataTypes/DataTypeString.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>

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
    extern const int UNKNOWN_DATABASE;
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
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::REDIS);
    SCOPE_EXIT({ session.reset(); });

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
                RedisProtocol::MGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "GET request for {} key", get_req.getKeys()[0]);

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                if (!table_ptr)
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("No database/table selected for %d", db));
                    resp.serialize(*out);
                    continue;
                }

                RedisProtocol::NilResponse resp;
                resp.serialize(*out);
            }
            else if (req.getMethod() == "mget")
            {
                RedisProtocol::MGetRequest get_req(req);
                get_req.deserialize(*in);
                LOG_DEBUG(log, "MGET request for {} keys", std::to_string(get_req.getKeys().size()));

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                if (!table_ptr)
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("No database/table selected for %d", db));
                    resp.serialize(*out);
                    continue;
                }

                // TODO: Complete me 
                ColumnWithTypeAndName keys(std::make_shared<DataTypeString>(), "keys");
                for (const auto & key : get_req.getKeys())
                {
                    keys.column.insert(key);
                }
                table_ptr->getByKeys(keys, );

                std::vector<std::optional<String>> hello_worlds(get_req.getKeys().size());
                for (size_t i = 0; i < get_req.getKeys().size(); ++i)
                {
                    hello_worlds[i] = std::nullopt;
                }
                RedisProtocol::ArrayResponse resp(hello_worlds);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "auth")
            {
                RedisProtocol::AuthRequest auth_req(req);
                auth_req.deserialize(*in);

                String username = auth_req.getUsername();
                if (auth_req.getUsername().empty())
                    username = "default";

                LOG_DEBUG(log, "AUTH request for {} username", auth_req.getUsername());

                RedisProtocol::Writer writer(out.get());
                if (authenticated)
                    writer.writeSimpleString(RedisProtocol::Message::OK);

                bool success
                    = authentication_manager.authenticate(username, auth_req.getPassword(), *session, socket().address(), out.get());

                authenticated = success;
            }
            else if (req.getMethod() == "select")
            {
                RedisProtocol::SelectRequest select_req(req);
                select_req.deserialize(*in);
                LOG_DEBUG(log, "SELECT request for {} db", std::to_string(select_req.getDb()));

                if (!authenticated)
                {
                    RedisProtocol::ErrorResponse resp(RedisProtocol::Message::NOAUTH);
                    resp.serialize(*out);
                    continue;
                }

                String db_name = server.config().getString(Poco::format("redis.db._%d.database", select_req.getDb()), "");
                String table_name = server.config().getString(Poco::format("redis.db._%d.table", select_req.getDb()), "");
                if (db_name.empty() || table_name.empty())
                {
                    RedisProtocol::ErrorResponse resp(Poco::format("Database or table is not set for %d", select_req.getDb()));
                    resp.serialize(*out);
                    continue;
                }

                if (!table_ptr || db != select_req.getDb())
                {
                    try
                    {
                        auto db_ptr = DatabaseCatalog::instance().getDatabase(db_name, server.context());
                        table_ptr = db_ptr->getTable(table_name, server.context());
                    }
                    catch (...)
                    {
                        RedisProtocol::ErrorResponse resp(Poco::format("Unknown database %s", db_name));
                        resp.serialize(*out);
                        continue;
                    }
                    db = select_req.getDb();
                }

                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
            }
            else if (req.getMethod() == "reset")
            {
                LOG_DEBUG(log, "RESET request");
                RedisProtocol::SimpleStringResponse resp("RESET");
                resp.serialize(*out);
            }
            else if (req.getMethod() == "quit")
            {
                LOG_DEBUG(log, "QUIT request");
                RedisProtocol::SimpleStringResponse resp(RedisProtocol::Message::OK);
                resp.serialize(*out);
                return;
            }
            else
            {
                throw Exception(
                    Poco::format("%s (%s)", RedisProtocol::Message::UNKNOWNCOMMAND, req.getMethod()), ErrorCodes::NOT_IMPLEMENTED);
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
