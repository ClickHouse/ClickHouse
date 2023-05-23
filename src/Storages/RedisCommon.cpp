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

RedisColumnTypes REDIS_HASH_MAP_COLUMN_TYPES = {RedisColumnType::KEY, RedisColumnType::FIELD, RedisColumnType::VALUE};
RedisColumnTypes REDIS_SIMPLE_COLUMN_TYPES = {RedisColumnType::KEY, RedisColumnType::VALUE};

RedisConnection::RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_)
    : pool(std::move(pool_)), client(std::move(client_))
{
}

RedisConnection::~RedisConnection()
{
    pool->returnObject(std::move(client));
}

String storageTypeToKeyType(RedisStorageType storage_type)
{
    switch (storage_type)
    {
        case RedisStorageType::SIMPLE:
            return "string";
        case RedisStorageType::HASH_MAP:
            return "hash";
        default:
            return "none";
    }
}

RedisStorageType keyTypeToStorageType(const String & key_type)
{
    if (key_type == "string")
        return RedisStorageType::SIMPLE;
    else if (key_type == "hash")
        return RedisStorageType::HASH_MAP;
    else
        return RedisStorageType::UNKNOWN;
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


RedisArrayPtr getRedisHashMapKeys(const RedisConnectionPtr & connection, RedisArray & keys)
{
    RedisArrayPtr hkeys = std::make_shared<RedisArray>();
    for (const auto & key : keys)
    {
        RedisCommand command_for_secondary_keys("HKEYS");
        command_for_secondary_keys.addRedisType(key);

        auto secondary_keys = connection->client->execute<RedisArray>(command_for_secondary_keys);

        RedisArray primary_with_secondary;
        primary_with_secondary.addRedisType(key);
        for (const auto & secondary_key : secondary_keys)
        {
            primary_with_secondary.addRedisType(secondary_key);
            /// Do not store more than max_block_size values for one request.
            if (primary_with_secondary.size() == REDIS_MAX_BLOCK_SIZE + 1)
            {
                hkeys->add(primary_with_secondary);
                primary_with_secondary.clear();
                primary_with_secondary.addRedisType(key);
            }
        }

        if (primary_with_secondary.size() > 1)
            hkeys->add(primary_with_secondary);
    }

    return hkeys;
}

}
