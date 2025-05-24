#include "RedisHandlerFactory.h"
#include "RedisHandler.h"
#include "RedisProtocolMapping.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Util/LayeredConfiguration.h>
#include "Common/Exception.h"
#include "Common/logger_useful.h"

namespace DB{
    RedisHandlerFactory::RedisHandlerFactory(IServer &_server) :
        server(_server), logger(&Poco::Logger::get("RedisHandlerFactory"))
    {
        parse_config();
    }

    Poco::Net::TCPServerConnection* RedisHandlerFactory::createConnection(const Poco::Net::StreamSocket &socket, TCPServer &tcp_server)
    {
        LOG_TRACE(logger, "Redis connection. Address: {}", socket.peerAddress().toString());
        return new RedisHandler(server, tcp_server, socket, config);
    }

    void RedisHandlerFactory::parse_config()
    {
        config.enable_ssl = server.config().getBool("redis.enable_ssl");

        Poco::Util::AbstractConfiguration::Keys keys;
        server.config().keys("redis.db", keys);
        for (auto& key : keys) {
            UInt32 db_num = static_cast<UInt32>(std::stoul(key.substr(1)));
            if (db_num >= RedisProtocol::DB_MAX_NUM) {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "db number {} is greater than the maximum allowed value {}",
                    db_num, RedisProtocol::DB_MAX_NUM
                );
            }

            String type = server.config().getString(std::format("redis.db.{}.db_type", key));
            auto db_type = RedisProtocol::toDBType(type);

            String clickhouse_db_name = server.config().getString(std::format("redis.db.{}.clickhouse_db", key));
            if (clickhouse_db_name.empty())
            {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "clickhouse_db not set for {}",
                    key
                );
            }
            auto db_ptr = DatabaseCatalog::instance().getDatabase(clickhouse_db_name, server.context());
            if (db_ptr == nullptr)
            {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "clickhouse_db {} not exists",
                    clickhouse_db_name
                );
            }

            String clickhouse_table_name = server.config().getString(std::format("redis.db.{}.clickhouse_table", key));
            if (clickhouse_table_name.empty())
            {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "clickhouse_table not set for {}",
                    key
                );
            }
            auto table_ptr = db_ptr->getTable(clickhouse_table_name, server.context());
            if (table_ptr == nullptr)
            {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "clickhouse_table {} not exists in db {}",
                    clickhouse_table_name, clickhouse_db_name
                );
            }

            String key_column = server.config().getString(std::format("redis.db.{}.key_column", key));
            if (key_column.empty())
            {
                throw Exception(
                    ErrorCodes::BAD_CONFIGURATION,
                    "key_column not set for {}",
                    key
                );
            }

            switch(db_type)
            {
                case RedisProtocol::DBType::STRING:
                {
                    String value_column = server.config().getString(std::format("redis.db.{}.value_column", key));
                    if (value_column.empty()) {
                        throw Exception(
                            ErrorCodes::BAD_CONFIGURATION,
                            "value_column not set for {}",
                            key
                        );
                    }
                    auto string_db = std::make_unique<RedisProtocol::RedisStringMapping>(db_type, table_ptr, key_column, value_column);
                    config.redis_click_house_mapping[db_num] = std::move(string_db);
                    break;
                }
                case RedisProtocol::DBType::HASH:
                {
                    auto hash_db = std::make_unique<RedisProtocol::RedisHashMapping>(db_type, table_ptr, key_column);
                    config.redis_click_house_mapping[db_num] = std::move(hash_db);
                    break;
                }
            }
        }
    }
}
