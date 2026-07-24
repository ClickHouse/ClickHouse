#pragma once

#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Array.h>
#include <Poco/Types.h>

#include <Core/Defines.h>
#include <base/BorrowedObjectPool.h>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFactory.h>

namespace DB
{
static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;

enum class RedisStorageType : uint8_t
{
    SIMPLE,
    HASH_MAP,
    UNKNOWN
};


/// storage type to Redis key type
String storageTypeToKeyType(RedisStorageType type);

RedisStorageType parseStorageType(const String & storage_type_str);
String serializeStorageType(RedisStorageType storage_type);

struct RedisConfiguration
{
    String host;
    uint32_t port;
    uint32_t db_index;
    String password;
    RedisStorageType storage_type;
    uint32_t pool_size;
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

///get all redis hash key array
///    eg: keys -> [key1, key2] and get [[key1, field1, field2], [key2, field1, field2]]
RedisArrayPtr getRedisHashMapKeys(const RedisConnectionPtr & connection, RedisArray & keys);

}
