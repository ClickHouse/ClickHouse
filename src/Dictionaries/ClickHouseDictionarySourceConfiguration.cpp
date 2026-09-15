#include <Dictionaries/ClickHouseDictionarySource.h>

#include <Common/isLocalAddress.h>
#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>


namespace DB
{

namespace
{
    UInt16 getPortFromContext(ContextPtr context, bool secure)
    {
        return secure ? context->getTCPPortSecure().value_or(0) : context->getTCPPort();
    }
}

ClickHouseDictionarySource::Configuration ClickHouseDictionarySource::resolveConfiguration(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    ContextPtr context,
    const std::string & default_database,
    bool created_from_ddl,
    NamedCollectionUsage named_collection_usage)
{
    const std::string settings_config_prefix = config_prefix + ".clickhouse";
    auto named_collection = created_from_ddl
        ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, context, named_collection_usage)
        : nullptr;

    if (named_collection)
    {
        validateNamedCollection(
            *named_collection, {}, ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>{
                "secure", "host", "hostname", "port", "user", "username", "password", "proto_send_chunked", "proto_recv_chunked", "quota_key", "name",
                "db", "database", "table","query", "where", "invalidate_query", "update_field", "update_lag"});

        const auto secure = named_collection->getOrDefault("secure", false);
        const auto default_port = getPortFromContext(context, secure);
        const auto host = named_collection->getAnyOrDefault<String>({"host", "hostname"}, "localhost");
        const auto port = static_cast<UInt16>(named_collection->getOrDefault<UInt64>("port", default_port));

        return Configuration{
            .host = host,
            .user = named_collection->getAnyOrDefault<String>({"user", "username"}, "default"),
            .password = named_collection->getOrDefault<String>("password", ""),
            .proto_send_chunked = named_collection->getOrDefault<String>("proto_send_chunked", "notchunked"),
            .proto_recv_chunked = named_collection->getOrDefault<String>("proto_recv_chunked", "notchunked"),
            .quota_key = named_collection->getOrDefault<String>("quota_key", ""),
            .db = named_collection->getAnyOrDefault<String>({"db", "database"}, default_database),
            .table = named_collection->getOrDefault<String>("table", ""),
            .query = named_collection->getOrDefault<String>("query", ""),
            .where = named_collection->getOrDefault<String>("where", ""),
            .invalidate_query = named_collection->getOrDefault<String>("invalidate_query", ""),
            .update_field = named_collection->getOrDefault<String>("update_field", ""),
            .update_lag = named_collection->getOrDefault<UInt64>("update_lag", 1),
            .port = port,
            .is_local = isLocalAddress({host, port}, default_port),
            .secure = secure,
        };
    }

    const auto secure = config.getBool(settings_config_prefix + ".secure", false);
    const auto default_port = getPortFromContext(context, secure);
    const auto host = config.getString(settings_config_prefix + ".host", "localhost");
    const auto port = static_cast<UInt16>(config.getUInt(settings_config_prefix + ".port", default_port));

    return Configuration{
        .host = host,
        .user = config.getString(settings_config_prefix + ".user", "default"),
        .password = config.getString(settings_config_prefix + ".password", ""),
        .proto_send_chunked = config.getString(settings_config_prefix + ".proto_caps.send", "notchunked"),
        .proto_recv_chunked = config.getString(settings_config_prefix + ".proto_caps.recv", "notchunked"),
        .quota_key = config.getString(settings_config_prefix + ".quota_key", ""),
        .db = config.getString(settings_config_prefix + ".db", default_database),
        .table = config.getString(settings_config_prefix + ".table", ""),
        .query = config.getString(settings_config_prefix + ".query", ""),
        .where = config.getString(settings_config_prefix + ".where", ""),
        .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
        .update_field = config.getString(settings_config_prefix + ".update_field", ""),
        .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
        .port = port,
        .is_local = isLocalAddress({host, port}, default_port),
        .secure = secure,
    };
}

}
