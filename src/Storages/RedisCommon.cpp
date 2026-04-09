#include <Storages/RedisCommon.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <base/sleep.h>

#include <Poco/Net/SocketAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INTERNAL_REDIS_ERROR;
    extern const int TIMEOUT_EXCEEDED;
    extern const int INVALID_REDIS_STORAGE_TYPE;
}

RedisConnection::RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_)
    : pool(std::move(pool_)), client(std::move(client_))
{
}

RedisConnection::~RedisConnection()
{
    if (pool)
        pool->returnObject(std::move(client));
}

String storageTypeToKeyType(RedisStorageType type)
{
    switch (type)
    {
        case RedisStorageType::SIMPLE:
            return "string";
        case RedisStorageType::HASH_MAP:
            return "hash";
        default:
            return "none";
    }

    UNREACHABLE();
}

String serializeStorageType(RedisStorageType storage_type)
{
    switch (storage_type)
    {
        case RedisStorageType::SIMPLE:
            return "simple";
        case RedisStorageType::HASH_MAP:
            return "hash_map";
        default:
            return "none";
    }
}

RedisStorageType parseStorageType(const String & storage_type_str)
{
    if (storage_type_str == "hash_map")
        return RedisStorageType::HASH_MAP;
    if (!storage_type_str.empty() && storage_type_str != "simple")
        throw Exception(ErrorCodes::INVALID_REDIS_STORAGE_TYPE, "Unknown storage type {} for Redis dictionary", storage_type_str);

    return RedisStorageType::SIMPLE;
}

String formatRedisEndpoint(const RedisEndpoint & endpoint)
{
    return endpoint.host + ":" + std::to_string(endpoint.port);
}

std::optional<RedisEndpoint> parseRedisEndpoint(const String & address, uint32_t default_port)
{
    try
    {
        const auto [host, port] = parseAddress(address, static_cast<UInt16>(default_port));
        return RedisEndpoint{host, port};
    }
    catch (...) // Ok: intentionally return nullopt on parse failure
    {
        return std::nullopt;
    }
}

std::vector<RedisEndpoint> getRedisStartupNodes(const RedisConfiguration & configuration)
{
    if (!configuration.startup_nodes.empty())
        return configuration.startup_nodes;

    return {{configuration.host, configuration.port}};
}

void connectRedisClient(Poco::Redis::Client & client, const RedisConfiguration & configuration, const RedisEndpoint & endpoint)
{
    auto connect_ts = Poco::Timespan(
        configuration.connect_timeout_ms / 1000,
        (configuration.connect_timeout_ms % 1000) * 1000);
    auto receive_ts = Poco::Timespan(
        configuration.receive_timeout_ms / 1000,
        (configuration.receive_timeout_ms % 1000) * 1000);

    client.connect(endpoint.host, endpoint.port, connect_ts);
    client.setReceiveTimeout(receive_ts);

    if (!configuration.password.empty())
    {
        RedisCommand command("AUTH");
        command << configuration.password;
        String reply = client.execute<String>(command);
        if (reply != "OK")
            throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                "Authentication failed for Redis endpoint {} with reason {}",
                formatRedisEndpoint(endpoint), reply);
    }

    if (configuration.topology_mode == RedisTopologyMode::Cluster)
    {
        if (configuration.db_index != 0)
            throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                "Redis Cluster only supports database index 0, got {}",
                configuration.db_index);
        return;
    }

    if (configuration.db_index != 0)
    {
        RedisCommand command("SELECT");
        command << std::to_string(configuration.db_index);
        String reply = client.execute<String>(command);
        if (reply != "OK")
            throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                "Selecting Redis database with index {} on endpoint {} failed with reason {}",
                configuration.db_index, formatRedisEndpoint(endpoint), reply);
    }
}

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration)
{
    const auto endpoints = getRedisStartupNodes(configuration);
    if (endpoints.empty())
        throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR, "No Redis endpoints configured");

    return getRedisConnection(pool, configuration, endpoints.front());
}

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration, const RedisEndpoint & endpoint)
{
    for (uint32_t attempt = 0; attempt <= configuration.max_retries; ++attempt)
    {
        RedisClientPtr client;
        bool ok = pool->tryBorrowObject(client,
            [] { return std::make_unique<Poco::Redis::Client>(); },
            REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

        if (!ok)
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED,
                            "Could not get connection from pool, timeout exceeded {} ms",
                            REDIS_LOCK_ACQUIRE_TIMEOUT_MS);

        if (!client->isConnected())
        {
            try
            {
                connectRedisClient(*client, configuration, endpoint);
            }
            catch (...)
            {
                if (client->isConnected())
                    client->disconnect();

                pool->returnObject(std::move(client));
                if (attempt < configuration.max_retries)
                {
                    if (configuration.retry_delay_ms > 0)
                        sleepForMilliseconds(configuration.retry_delay_ms);
                    continue;
                }

                throw;
            }
        }
        else
        {
            /// Validate that the pooled connection is still alive with a PING.
            /// If the connection is stale (e.g. Redis was restarted), disconnect
            /// and reconnect transparently.
            try
            {
                RedisCommand ping_cmd("PING");
                client->execute<String>(ping_cmd);
            }
            /// Ok: PING failure is handled below by reconnecting.
            catch (...)
            {
                /// Connection is broken — disconnect and let the next borrow reconnect.
                try
                {
                    client->disconnect();
                }
                catch (...)
                {
                    tryLogCurrentException("RedisConnection", "Failed to disconnect broken Redis connection");
                }
                pool->returnObject(std::move(client));
                /// Retry with a fresh connection.
                if (attempt < configuration.max_retries && configuration.retry_delay_ms > 0)
                    sleepForMilliseconds(configuration.retry_delay_ms);
                continue;
            }
        }

        return std::make_unique<RedisConnection>(pool, std::move(client));
    }

    throw Exception(ErrorCodes::INTERNAL_REDIS_ERROR,
                    "Failed to get a valid Redis connection after {} retries", configuration.max_retries);
    UNREACHABLE();
}


RedisArrayPtr getRedisHashMapKeys(const RedisConnectionPtr & connection, RedisArray & keys)
{
    RedisArrayPtr hkeys = std::make_shared<RedisArray>();
    for (const auto & key : keys)
    {
        RedisCommand command_for_secondary_keys("HKEYS");
        command_for_secondary_keys.addRedisType(key);

        auto secondary_keys = connection->client->execute<RedisArray>(command_for_secondary_keys);
        if (secondary_keys.isNull())
            continue;

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
