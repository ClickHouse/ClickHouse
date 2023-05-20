#include "RedisCommon.h"
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_REDIS_STORAGE_TYPE;
    extern const int INTERNAL_REDIS_ERROR;
    extern const int TIMEOUT_EXCEEDED;
}

RedisConnection::RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_)
    : pool(std::move(pool_)), client(std::move(client_))
{
}

RedisConnection::~RedisConnection()
{
    pool->returnObject(std::move(client));
}

String toString(RedisStorageType storage_type)
{
    static const std::unordered_map<RedisStorageType, String> type_to_str_map
        = {{RedisStorageType::SIMPLE, "simple"}, {RedisStorageType::HASH_MAP, "hash_map"}};

    auto iter = type_to_str_map.find(storage_type);
    return iter->second;
}

RedisStorageType toRedisStorageType(const String & storage_type)
{
    static const std::unordered_map<std::string, RedisStorageType> str_to_type_map
        = {{"simple", RedisStorageType::SIMPLE}, {"hash", RedisStorageType::HASH_MAP}};

    auto iter = str_to_type_map.find(storage_type);
    if (iter == str_to_type_map.end())
    {
        throw Exception(ErrorCodes::INVALID_REDIS_STORAGE_TYPE, "invalid redis storage type: {}", storage_type);
    }
    return iter->second;
}

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration)
{
    RedisClientPtr client;
    bool ok = pool->tryBorrowObject(client,
        [] { return std::make_unique<Poco::Redis::Client>(); },
        REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

    if (!ok)
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                        "Could not get connection from pool, timeout exceeded {} seconds",
                        REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

    if (!client->isConnected())
    {
        try
        {
            client->connect(configuration.host, configuration.port);

            if (!configuration.password.empty())
            {
                RedisCommand command("AUTH");
                command << configuration.password;
                String reply = client->execute<String>(command);
                if (reply != "OK")
                    throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                                    "Authentication failed with reason {}", reply);
            }

            if (configuration.db_index != 0)
            {
                RedisCommand command("SELECT");
                command << std::to_string(configuration.db_index);
                String reply = client->execute<String>(command);
                if (reply != "OK")
                    throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                                    "Selecting database with index {} failed with reason {}",
                                    configuration.db_index, reply);
            }
        }
        catch (...)
        {
            if (client->isConnected())
                client->disconnect();

            pool->returnObject(std::move(client));
            throw;
        }
    }

    return std::make_unique<RedisConnection>(pool, std::move(client));
}

}
