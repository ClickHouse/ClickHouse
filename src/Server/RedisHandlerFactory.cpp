#include <Poco/Util/LayeredConfiguration.h>
#include <memory>

#include "RedisHandlerFactory.h"
#include "RedisHandler.h"
#include "RedisProtocolMapping.h"
#include "Common/Exception.h"
#include "Common/logger_useful.h"

namespace DB
{
    namespace ErrorCodes
    {
        extern const int INVALID_CONFIG_PARAMETER;
    }

    RedisHandlerFactory::RedisHandlerFactory(IServer &_server) :
        server(_server), logger(&Poco::Logger::get("RedisHandlerFactory"))
    {
        config = std::make_shared<RedisProtocol::Config>();
        parse_config();
    }

    Poco::Net::TCPServerConnection* RedisHandlerFactory::createConnection(const Poco::Net::StreamSocket &socket, TCPServer &tcp_server)
    {
        LOG_TRACE(logger, "Redis connection. Address: {}", socket.peerAddress().toString());
        return new RedisHandler(server, tcp_server, socket, config);
    }

    void RedisHandlerFactory::parse_config()
    {
        config->enable_ssl = server.config().getBool("redis.enable_ssl");

        Poco::Util::AbstractConfiguration::Keys keys;
        server.config().keys("redis.db", keys);
        for (auto& key : keys)
        {
            UInt32 db_num = static_cast<UInt32>(std::stoul(key.substr(1)));
            if (db_num >= RedisProtocol::DB_MAX_NUM)
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "db number {} is greater than the maximum allowed value {}",
                    db_num, RedisProtocol::DB_MAX_NUM
                );
            }

            RedisProtocol::MapDescription description;

            String type = server.config().getString(fmt::format("redis.db.{}.db_type", key));
            description.db_type = RedisProtocol::toDBType(type);

            description.clickhouse_db = server.config().getString(fmt::format("redis.db.{}.clickhouse_db", key));
            if (description.clickhouse_db.empty())
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "clickhouse_db not set for {}",
                    key
                );
            }


            description.clickhouse_table = server.config().getString(fmt::format("redis.db.{}.clickhouse_table", key));
            if (description.clickhouse_table.empty())
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "clickhouse_table not set for {}",
                    key
                );
            }


            description.key_column = server.config().getString(fmt::format("redis.db.{}.key_column", key));
            if (description.key_column.empty())
            {
                throw Exception(
                    ErrorCodes::INVALID_CONFIG_PARAMETER,
                    "key_column not set for {}",
                    key
                );
            }

            switch (description.db_type)
            {
                case RedisProtocol::DBType::STRING:
                {
                    description.value_column = server.config().getString(fmt::format("redis.db.{}.value_column", key));
                    if (description.value_column.empty())
                    {
                        throw Exception(
                            ErrorCodes::INVALID_CONFIG_PARAMETER,
                            "value_column not set for {}",
                            key
                        );
                    }
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
