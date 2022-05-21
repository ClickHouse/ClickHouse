#include "RedisHandlerFactory.h"
#include "RedisHandler.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Util/LayeredConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE;
}

RedisHandlerFactory::RedisHandlerFactory(IServer & server_) : server(server_), log(&Poco::Logger::get("RedisHandlerFactory"))
{
    bool set = false;
    for (int i = 0; i < 16; ++i)
    {
        String db_name = server.config().getString(Poco::format("redis.db._%d.database", i), "");
        String table_name = server.config().getString(Poco::format("redis.db._%d.table", i), "");
        if (!db_name.empty())
        {
            auto db_ptr = DatabaseCatalog::instance().getDatabase(db_name, server.context());
            db_ptr->getTable(table_name, server.context());
            set = true;
        }
    }
    if (!set)
        throw Exception("At least one database must be configured for redis", ErrorCodes::UNKNOWN_DATABASE);
}

Poco::Net::TCPServerConnection * RedisHandlerFactory::createConnection(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    LOG_TRACE(log, "Redis connection. Address: {}", socket.peerAddress().toString());
    return new RedisHandler(socket, server, tcp_server);
}

}
