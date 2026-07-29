#pragma once

#include <map>
#include <memory>

#include <Poco/String.h>

#include <base/types.h>
#include <Interpreters/StorageID.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace RedisProtocol
{

static constexpr UInt32 DB_MAX_NUM = 256;

enum class DBType : uint8_t
{
    STRING,
    HASH,
};

inline DBType toDBType(const String & type)
{
    auto upper_type = Poco::toUpper(type);
    if (upper_type == "STRING")
        return DBType::STRING;
    if (upper_type == "HASH")
        return DBType::HASH;
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown Redis database type {}", type);
}

/// Binding of a Redis database number to a ClickHouse table.
/// Only the table name is kept: the table is resolved anew for every request, so that
/// `DROP TABLE`, `RENAME TABLE`, or drop-and-recreate are visible to already connected clients.
class RedisClickHouseMapping
{
public:
    RedisClickHouseMapping(DBType type_, StorageID table_id_, const String & key_column_)
        : type(type_), table_id(std::move(table_id_)), key_column(key_column_)
    {
    }

    virtual ~RedisClickHouseMapping() = default;

    DBType getType() const { return type; }

    const StorageID & getTableID() const { return table_id; }

    const String & getKeyColumnName() const { return key_column; }

protected:
    DBType type;
    StorageID table_id;
    String key_column;
};

using MappingPtr = std::shared_ptr<RedisClickHouseMapping>;

class RedisStringMapping : public RedisClickHouseMapping
{
public:
    RedisStringMapping(DBType type_, StorageID table_id_, const String & key_column_, const String & value_column_)
        : RedisClickHouseMapping(type_, std::move(table_id_), key_column_), value_column(value_column_)
    {
    }

    String getValueColumnName() const { return value_column; }

private:
    String value_column;
};

class RedisHashMapping : public RedisClickHouseMapping
{
public:
    RedisHashMapping(DBType type_, StorageID table_id_, const String & key_column_)
        : RedisClickHouseMapping(type_, std::move(table_id_), key_column_)
    {
    }
};

/// Description of a Redis database mapping from the server configuration.
struct MapDescription
{
    DBType db_type = DBType::STRING;
    String clickhouse_db;
    String clickhouse_table;
    String key_column;
    String value_column;
};

struct Config
{
    std::map<UInt32, MapDescription> db_mapping;
};

using ConfigPtr = std::shared_ptr<Config>;

}

}
