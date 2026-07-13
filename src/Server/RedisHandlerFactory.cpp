#include <memory>

#include <Poco/Net/StreamSocket.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <IO/ReadHelpers.h>
#include <Server/RedisHandler.h>
#include <Server/RedisHandlerFactory.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

RedisHandlerFactory::RedisHandlerFactory(IServer & server_)
    : server(server_), log(getLogger("RedisHandlerFactory"))
{
    config = std::make_shared<RedisProtocol::Config>();
    parseConfig();
}

Poco::Net::TCPServerConnection * RedisHandlerFactory::createConnectionImpl(const Poco::Net::StreamSocket & socket, TCPServer & tcp_server)
{
    LOG_TRACE(log, "Redis connection. Address: {}", socket.peerAddress().toString());
    return new RedisHandler(server, tcp_server, socket, config);
}

void RedisHandlerFactory::parseConfig()
{
    Poco::Util::AbstractConfiguration::Keys keys;
    server.config().keys("redis.db", keys);
    for (const auto & key : keys)
    {
        /// Config keys cannot start with a digit, so the database number is written as e.g. `_0`.
        UInt32 db_num = parse<UInt32>(key.substr(1));
        if (db_num >= RedisProtocol::DB_MAX_NUM)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Redis database number {} is greater than the maximum allowed value {}",
                db_num, RedisProtocol::DB_MAX_NUM);

        RedisProtocol::MapDescription description;

        String type = server.config().getString(fmt::format("redis.db.{}.db_type", key));
        description.db_type = RedisProtocol::toDBType(type);

        description.clickhouse_db = server.config().getString(fmt::format("redis.db.{}.clickhouse_db", key));
        if (description.clickhouse_db.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "clickhouse_db is not set for Redis database {}", key);

        description.clickhouse_table = server.config().getString(fmt::format("redis.db.{}.clickhouse_table", key));
        if (description.clickhouse_table.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "clickhouse_table is not set for Redis database {}", key);

        description.key_column = server.config().getString(fmt::format("redis.db.{}.key_column", key));
        if (description.key_column.empty())
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "key_column is not set for Redis database {}", key);

        switch (description.db_type)
        {
            case RedisProtocol::DBType::STRING:
            {
                description.value_column = server.config().getString(fmt::format("redis.db.{}.value_column", key));
                if (description.value_column.empty())
                    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "value_column is not set for Redis database {}", key);
                break;
            }
            case RedisProtocol::DBType::HASH:
            {
                break;
            }
        }

        config->db_mapping[db_num] = std::move(description);
    }
}

}
