#pragma once

#include <memory>
#include <Poco/String.h>

#include "Common/Exception.h"
#include "Databases/IDatabase.h"
#include "Storages/IStorage_fwd.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace RedisProtocol
{
    const UInt32 DB_MAX_NUM = 256;

    enum class DBType
    {
        STRING,
        HASH,
    };

    inline DBType toDBType(String type)
    {
        auto tp = Poco::toUpper(type);
        if (tp == "STRING")
        {
            return DBType::STRING;
        }
        else if (tp == "HASH")
        {
            return DBType::HASH;
        }
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Unknown db type {}", type
        );
    }

    class RedisClickHouseMapping
    {
    public:
        RedisClickHouseMapping(DBType type_, StoragePtr table_, String key_column_)
            : type(type_), table(table_), key_column(key_column_) {}

        virtual ~RedisClickHouseMapping() = default;

        DBType getType() { return type; }

        StoragePtr getTable() { return table; }

    protected:
        DBType type;
        StoragePtr table;
        String key_column;
    };

    using MappingPtr = std::shared_ptr<RedisClickHouseMapping>;

    class RedisStringMapping : public RedisClickHouseMapping
    {
    public:
        RedisStringMapping(DBType type_, StoragePtr table_, String key_column_, String value_column_)
            : RedisClickHouseMapping(type_, table_, key_column_), value_column(value_column_) {}

        String getValueColumnName() { return value_column; }
    private:
        String value_column;
    };

    class RedisHashMapping : public RedisClickHouseMapping
    {
    public:
        RedisHashMapping(DBType type_, StoragePtr table_, String key_column_)
            : RedisClickHouseMapping(type_, table_, key_column_) {}
    };

    struct MapDescription
    {
        DBType db_type;
        String clickhouse_db;
        String clickhouse_table;
        String key_column;
        String value_column;
    };

    struct Config
    {
        bool enable_ssl;
        std::map<UInt32, MapDescription> db_mapping;
    };

    using ConfigPtr = std::shared_ptr<Config>;
}
}
