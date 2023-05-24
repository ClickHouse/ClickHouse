#pragma once

#include <Poco/Redis/Client.h>
#include <Poco/Redis/Command.h>
#include <Poco/Redis/Array.h>

#include <Core/Defines.h>
#include <base/BorrowedObjectPool.h>
#include <Core/Names.h>
#include <Storages/ColumnsDescription.h>

namespace DB
{
static constexpr size_t REDIS_MAX_BLOCK_SIZE = DEFAULT_BLOCK_SIZE;
static constexpr size_t REDIS_LOCK_ACQUIRE_TIMEOUT_MS = 5000;

enum class RedisStorageType
{
    SIMPLE,
    HASH_MAP,
    UNKNOWN
};

enum class RedisColumnType
{
    /// Redis key
    KEY,
    /// Redis hash field
    FIELD,
    /// Redis value
    VALUE
};

using RedisColumnTypes = std::vector<RedisColumnType>;

extern RedisColumnTypes REDIS_HASH_MAP_COLUMN_TYPES;
extern RedisColumnTypes REDIS_SIMPLE_COLUMN_TYPES;

String storageTypeToKeyType(RedisStorageType storage_type);
RedisStorageType keyTypeToStorageType(const String & key_type);

struct RedisConfiguration
{
    String host;
    uint32_t port;
    uint32_t db_index;
    String password;
    RedisStorageType storage_type;
    uint32_t pool_size;
};

using RedisArray = Poco::Redis::Array;
using RedisArrayPtr = std::shared_ptr<RedisArray>;
using RedisCommand = Poco::Redis::Command;
using RedisBulkString = Poco::Redis::BulkString;

using RedisClientPtr = std::unique_ptr<Poco::Redis::Client>;
using RedisPool = BorrowedObjectPool<RedisClientPtr>;
using RedisPoolPtr = std::shared_ptr<RedisPool>;

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

/// Get RedisColumnType of a column, If storage_type is
///     SIMPLE: all_columns must have 2 items and the first one is Redis key the second one is value
///     HASH_MAP: all_columns must have 2 items and the first one is Redis key the second is field, the third is value.
RedisColumnType getRedisColumnType(RedisStorageType storage_type, const Names & all_columns, const String & column);

/// checking Redis table/table-function when creating
void checkRedisTableStructure(const ColumnsDescription & columns, const RedisConfiguration & configuration);

}
