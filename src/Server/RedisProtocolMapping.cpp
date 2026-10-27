#include <Server/RedisProtocolMapping.h>

#include <Poco/String.h>

#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

namespace RedisProtocol
{

DBType toDBType(const String & type)
{
    auto upper_type = Poco::toUpper(type);
    if (upper_type == "STRING")
        return DBType::STRING;
    if (upper_type == "HASH")
        return DBType::HASH;
    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Unknown Redis database type {}", type);
}

namespace
{

String getRequiredString(const Poco::Util::AbstractConfiguration & config, const String & prefix, const String & name, UInt32 db_num)
{
    auto value = config.getString(fmt::format("{}.{}", prefix, name), "");
    if (value.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "{} is not set for Redis database {}", name, db_num);
    return value;
}

MapDescription parseDescription(const Poco::Util::AbstractConfiguration & config, const String & prefix, UInt32 db_num)
{
    MapDescription description;

    description.db_type = toDBType(getRequiredString(config, prefix, "db_type", db_num));
    description.clickhouse_db = getRequiredString(config, prefix, "clickhouse_db", db_num);
    description.clickhouse_table = getRequiredString(config, prefix, "clickhouse_table", db_num);
    description.key_column = getRequiredString(config, prefix, "key_column", db_num);

    switch (description.db_type)
    {
        case DBType::STRING:
        {
            description.value_column = getRequiredString(config, prefix, "value_column", db_num);
            break;
        }
        case DBType::HASH:
        {
            break;
        }
    }

    return description;
}

UInt32 parseDBKey(const String & key)
{
    if (!key.starts_with('_'))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Unexpected key redis.db.{}: a Redis database number has to be written with a leading underscore, e.g. `_0`",
            key);

    UInt32 db_num = parse<UInt32>(key.substr(1));

    /// Only the canonical spelling is accepted: a key such as `_01` would otherwise pass the validation
    /// as database 1, but would never be found by the lookup of a single database.
    if (key != fmt::format("_{}", db_num))
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Unexpected key redis.db.{}: a Redis database number has to be written without leading zeros, e.g. `_{}`",
            key, db_num);

    if (db_num >= DB_MAX_NUM)
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Redis database number {} is out of range, the maximum allowed value is {}",
            db_num, DB_MAX_NUM - 1);

    return db_num;
}

}

std::optional<MapDescription> parseDBDescription(const Poco::Util::AbstractConfiguration & config, UInt32 db_num)
{
    /// Validate the whole section with the same rules as at startup: the configuration may have been
    /// changed by `SYSTEM RELOAD CONFIG` since then.
    auto db_mapping = parseConfig(config);
    auto it = db_mapping.find(db_num);
    if (it == db_mapping.end())
        return {};
    return std::move(it->second);
}

std::map<UInt32, MapDescription> parseConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::map<UInt32, MapDescription> db_mapping;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("redis.db", keys);
    for (const auto & key : keys)
    {
        UInt32 db_num = parseDBKey(key);
        db_mapping[db_num] = parseDescription(config, fmt::format("redis.db.{}", key), db_num);
    }

    return db_mapping;
}

}

}
