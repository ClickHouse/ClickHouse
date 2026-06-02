#pragma once

#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Array.h>
#include <Poco/Types.h>

#include <Core/Defines.h>
#include <Common/SipHash.h>
#include <base/BorrowedObjectPool.h>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFactory.h>

#include <optional>

namespace DB
{
static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;
static constexpr uint32_t DEFAULT_REDIS_CONNECT_TIMEOUT_MS = 2000;
static constexpr uint32_t DEFAULT_REDIS_RECEIVE_TIMEOUT_MS = 2000;
static constexpr uint32_t DEFAULT_REDIS_MAX_RETRIES = 3;
static constexpr uint32_t DEFAULT_REDIS_RETRY_DELAY_MS = 100;

enum class RedisStorageType : uint8_t
{
    SIMPLE,
    HASH_MAP,
    UNKNOWN
};

enum class RedisTopologyMode : uint8_t
{
    Standalone,
    Cluster,
};

struct RedisEndpoint
{
    String host;
    uint32_t port = 0;

    bool operator==(const RedisEndpoint & other) const = default;
};

struct RedisEndpointHash
{
    size_t operator()(const RedisEndpoint & endpoint) const
    {
        SipHash hash;
        hash.update(endpoint.host);
        hash.update(endpoint.port);
        return hash.get64();
    }
};


/// storage type to Redis key type
String storageTypeToKeyType(RedisStorageType type);

RedisStorageType parseStorageType(const String & storage_type_str);
String serializeStorageType(RedisStorageType storage_type);

struct RedisConfiguration
{
    String host;
    uint32_t port{};
    uint32_t db_index{};
    String password;
    uint32_t connect_timeout_ms = DEFAULT_REDIS_CONNECT_TIMEOUT_MS;
    uint32_t receive_timeout_ms = DEFAULT_REDIS_RECEIVE_TIMEOUT_MS;
    uint32_t max_retries = DEFAULT_REDIS_MAX_RETRIES;
    uint32_t retry_delay_ms = DEFAULT_REDIS_RETRY_DELAY_MS;
    RedisTopologyMode topology_mode = RedisTopologyMode::Standalone;
    std::vector<RedisEndpoint> startup_nodes;
    RedisStorageType storage_type{};
    uint32_t pool_size{};
};

static uint32_t DEFAULT_REDIS_DB_INDEX = 0;
static uint32_t DEFAULT_REDIS_POOL_SIZE = 16;
static String DEFAULT_REDIS_PASSWORD;

using RedisCommand = Poco::Redis::Command;
using RedisArray = Poco::Redis::Array;
using RedisArrayPtr = std::shared_ptr<RedisArray>;
using RedisBulkString = Poco::Redis::BulkString;
using RedisSimpleString = String;
using RedisInteger = Poco::Int64;

using RedisClientPtr = std::unique_ptr<Poco::Redis::Client>;
using RedisPool = BorrowedObjectPool<RedisClientPtr>;
using RedisPoolPtr = std::shared_ptr<RedisPool>;

/// Redis scan iterator
using RedisIterator = int64_t;

struct RedisConnection
{
    RedisConnection(RedisPoolPtr pool_, RedisClientPtr client_);
    ~RedisConnection();

    RedisPoolPtr pool;
    RedisClientPtr client;
};

using RedisConnectionPtr = std::unique_ptr<RedisConnection>;

RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration);
RedisConnectionPtr getRedisConnection(RedisPoolPtr pool, const RedisConfiguration & configuration, const RedisEndpoint & endpoint);
void connectRedisClient(Poco::Redis::Client & client, const RedisConfiguration & configuration, const RedisEndpoint & endpoint);
std::vector<RedisEndpoint> getRedisStartupNodes(const RedisConfiguration & configuration);
String formatRedisEndpoint(const RedisEndpoint & endpoint);
std::optional<RedisEndpoint> parseRedisEndpoint(const String & address, uint32_t default_port = 6379);

///get all redis hash key array
///    eg: keys -> [key1, key2] and get [[key1, field1, field2], [key2, field1, field2]]
RedisArrayPtr getRedisHashMapKeys(const RedisConnectionPtr & connection, RedisArray & keys);

}
