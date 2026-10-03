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

}

std::optional<MapDescription> parseDBDescription(const Poco::Util::AbstractConfiguration & config, UInt32 db_num)
{
    /// Config keys cannot start with a digit, so the database number is written as e.g. `_0`.
    const String prefix = fmt::format("redis.db._{}", db_num);

    /// `has` does not work for a section without a value of its own, so look at its keys instead.
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);
    if (keys.empty())
        return {};

    return parseDescription(config, prefix, db_num);
}

std::map<UInt32, MapDescription> parseConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::map<UInt32, MapDescription> db_mapping;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys("redis.db", keys);
    for (const auto & key : keys)
    {
        if (!key.starts_with('_'))
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Unexpected key redis.db.{}: a Redis database number has to be written with a leading underscore, e.g. `_0`",
                key);

        UInt32 db_num = parse<UInt32>(key.substr(1));
        if (db_num >= DB_MAX_NUM)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER,
                "Redis database number {} is greater than the maximum allowed value {}",
                db_num, DB_MAX_NUM);

        db_mapping[db_num] = parseDescription(config, fmt::format("redis.db.{}", key), db_num);
    }

    return db_mapping;
}

}

}
