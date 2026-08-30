#pragma once

#include <map>
#include <optional>

#include <Poco/Util/AbstractConfiguration.h>

#include <base/types.h>

namespace DB
{

namespace RedisProtocol
{

static constexpr UInt32 DB_MAX_NUM = 256;

enum class DBType : uint8_t
{
    STRING,
    HASH,
};

DBType toDBType(const String & type);

/// Binding of a Redis database number to a ClickHouse table, as written in the server configuration.
/// Only names are kept: both the configuration and the table are read anew for every request, so that
/// `SYSTEM RELOAD CONFIG` and `DROP TABLE` / `RENAME TABLE` / drop-and-recreate are visible to
/// already connected clients.
struct MapDescription
{
    DBType db_type = DBType::STRING;
    String clickhouse_db;
    String clickhouse_table;
    String key_column;
    String value_column;
};

/// Reads the binding of a single Redis database number. Returns nullopt if the database is not configured.
std::optional<MapDescription> parseDBDescription(const Poco::Util::AbstractConfiguration & config, UInt32 db_num);

/// Reads the bindings of all the configured Redis databases. Used to validate the configuration.
std::map<UInt32, MapDescription> parseConfig(const Poco::Util::AbstractConfiguration & config);

}

}
