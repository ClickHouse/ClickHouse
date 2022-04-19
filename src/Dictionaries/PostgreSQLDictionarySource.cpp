#include "PostgreSQLDictionarySource.h"

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/QualifiedTableName.h>
#include "DictionarySourceFactory.h"
#include "registerDictionaries.h"

#if USE_LIBPQXX
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Transforms/PostgreSQLSource.h>
#include "readInvalidateQuery.h"
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Storages/ExternalDataSourceConfiguration.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

#if USE_LIBPQXX

static const UInt64 max_block_size = 8192;

static const std::unordered_map<String, ConfigKeyInfo> dictionary_keys =
{
    {"host", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"port", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::UInt64) }},
    {"user", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"password", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"db", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"database", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"table", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"schema", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"update_field", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"update_tag", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"invalidate_query", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"query", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"where", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"name", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::String) }},
    {"priority", ConfigKeyInfo{ .which = WhichDataType(TypeIndex::UInt64) }}
};

namespace
{
    ExternalQueryBuilder makeExternalQueryBuilder(const DictionaryStructure & dict_struct, const String & schema, const String & table, const String & query, const String & where)
    {
        QualifiedTableName qualified_name{schema, table};

        if (qualified_name.database.empty() && !qualified_name.table.empty())
            qualified_name = QualifiedTableName::parseFromString(qualified_name.table);

        /// Do not need db because it is already in a connection string.
        return {dict_struct, "", qualified_name.database, qualified_name.table, query, where, IdentifierQuotingStyle::DoubleQuotes};
    }
}


PostgreSQLDictionarySource::PostgreSQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    postgres::PoolWithFailoverPtr pool_,
    const Block & sample_block_)
    : dict_struct(dict_struct_)
    , configuration(configuration_)
    , pool(std::move(pool_))
    , sample_block(sample_block_)
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.query, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
{
}


/// copy-constructor is provided in order to support cloneability
PostgreSQLDictionarySource::PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other)
    : dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , pool(other.pool)
    , sample_block(other.sample_block)
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.query, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
    , update_time(other.update_time)
    , invalidate_query_response(other.invalidate_query_response)
{
}


Pipe PostgreSQLDictionarySource::loadAll()
{
    LOG_TRACE(log, fmt::runtime(load_all_query));
    return loadBase(load_all_query);
}


Pipe PostgreSQLDictionarySource::loadUpdatedAll()
{
    auto load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, fmt::runtime(load_update_query));
    return loadBase(load_update_query);
}

Pipe PostgreSQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadBase(query);
}


Pipe PostgreSQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadBase(query);
}


Pipe PostgreSQLDictionarySource::loadBase(const String & query)
{
    return Pipe(std::make_shared<PostgreSQLSource<>>(pool->get(), query, sample_block, max_block_size));
}


bool PostgreSQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        if (response == invalidate_query_response) //-V1051
            return false;
        invalidate_query_response = response;
    }
    return true;
}


std::string PostgreSQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    return readInvalidateQuery(QueryPipeline(std::make_unique<PostgreSQLSource<>>(pool->get(), request, invalidate_sample_block, 1)));
}


bool PostgreSQLDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}


std::string PostgreSQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(configuration.update_field, str_time);
    }
    else
    {
        update_time = std::chrono::system_clock::now();
        return query_builder.composeLoadAllQuery();
    }
}


bool PostgreSQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}


DictionarySourcePtr PostgreSQLDictionarySource::clone() const
{
    return std::make_shared<PostgreSQLDictionarySource>(*this);
}


std::string PostgreSQLDictionarySource::toString() const
{
    const auto & where = configuration.where;
    return "PostgreSQL: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

#endif

static void getOverridenResultForNamedCollection(const String & key, String & value, ConfigurationFromNamedCollection & overriding_configuration, ConfigurationFromNamedCollection & configuration)
{
    String overriding_value, config_value;
    if (overriding_configuration.contains(key))
        overriding_value = overriding_configuration[key].safeGet<String>();

    if (configuration.contains(key))
        config_value = configuration[key].safeGet<String>();

    value = overriding_value.empty() ? config_value : overriding_value;
}

static void getOverridenResultForNamedCollection(const String & key, UInt64 & value, ConfigurationFromNamedCollection & overriding_configuration, ConfigurationFromNamedCollection & configuration)
{
    UInt64 overriding_value = 0, config_value = 0;
    if (overriding_configuration.contains(key))
        overriding_value = overriding_configuration[key].safeGet<UInt64>();

    if (configuration.contains(key))
        config_value = configuration[key].safeGet<UInt64>();

    value = overriding_value == 0 ? config_value : overriding_value;
}

void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool /* created_from_ddl */) -> DictionarySourcePtr
    {
#if USE_LIBPQXX
        const auto settings_config_prefix = config_prefix + ".postgresql";

        std::optional<PostgreSQLDictionarySource::Configuration> dictionary_configuration;
        std::vector<postgres::PoolWithFailover::AuthSettings> connection_info;

        if (isNamedCollection(config, settings_config_prefix))
        {
            auto [root_configuration, replicas_configurations] = getListedConfigurationFromNamedCollection(
                getCollectionName(config, settings_config_prefix), context->getConfigRef(), dictionary_keys, "replica");

            validateConfigKeys(config, settings_config_prefix, dictionary_keys, "replica");
            auto overriding_configuration = parseConfigKeys(config, settings_config_prefix, dictionary_keys);

            String database, schema, username, host, password, table, query;
            UInt64 port, priority;

            getOverridenResultForNamedCollection("database", database, overriding_configuration, root_configuration);
            getOverridenResultForNamedCollection("schema", schema, overriding_configuration, root_configuration);
            getOverridenResultForNamedCollection("table", table, overriding_configuration, root_configuration);
            getOverridenResultForNamedCollection("query", query, overriding_configuration, root_configuration);

            if (database.empty())
                database = root_configuration["db"].safeGet<String>();

            auto configurations = replicas_configurations.empty() ? std::vector<ConfigurationFromNamedCollection>{ root_configuration } : replicas_configurations;
            for (auto & replica_configuration : configurations)
            {
                getOverridenResultForNamedCollection("user", username, overriding_configuration, replica_configuration);
                getOverridenResultForNamedCollection("host", host, overriding_configuration, replica_configuration);
                getOverridenResultForNamedCollection("port", port, overriding_configuration, replica_configuration);
                getOverridenResultForNamedCollection("password", password, overriding_configuration, replica_configuration);
                getOverridenResultForNamedCollection("priority", priority, overriding_configuration, replica_configuration);

                connection_info.emplace_back(
                    postgres::PoolWithFailover::AuthSettings{ .database = database, .host = host, .port = static_cast<UInt16>(port), .username = username, .password = password, .priority = priority });
            }

            dictionary_configuration.emplace(PostgreSQLDictionarySource::Configuration{
                .db = database,
                .schema = schema,
                .table = table,
                .query = query,
                .where = config.getString(fmt::format("{}.where", settings_config_prefix), ""),
                .invalidate_query = config.getString(fmt::format("{}.invalidate_query", settings_config_prefix), ""),
                .update_field = config.getString(fmt::format("{}.update_field", settings_config_prefix), ""),
                .update_lag = config.getUInt64(fmt::format("{}.update_lag", settings_config_prefix), 1)
            });
        }
        else
        {
            validateConfigKeys(config, settings_config_prefix, dictionary_keys, "replica");

            auto database = config.getString(fmt::format("{}.db", settings_config_prefix), config.getString(fmt::format("{}.database", settings_config_prefix), ""));

            auto root_configuration = postgres::PoolWithFailover::AuthSettings{
                .database = database,
                .host = config.getString(fmt::format("{}.host", settings_config_prefix), ""),
                .port = static_cast<UInt16>(config.getUInt(fmt::format("{}.port", settings_config_prefix), 0)),
                .username = config.getString(fmt::format("{}.username", settings_config_prefix), config.getString(fmt::format("{}.user", settings_config_prefix), "")),
                .password = config.getString(fmt::format("{}.password", settings_config_prefix), "")
            };

            if (config.has(settings_config_prefix + ".replica"))
            {
                Poco::Util::AbstractConfiguration::Keys config_keys;
                config.keys(settings_config_prefix, config_keys);

                for (const auto & config_key : config_keys)
                {
                    if (!dictionary_keys.contains(config_key) && !config_key.starts_with("replica"))
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument `{}` in dictionary config", config_key);

                    if (config_key.starts_with("replica"))
                    {
                        String replica_name = settings_config_prefix + "." + config_key;

                        size_t priority = config.getInt(replica_name + ".priority", 0);
                        auto host = config.getString(replica_name + ".host", config.getString(replica_name + ".hostname", root_configuration.host));
                        auto port = config.getUInt(replica_name + ".port", root_configuration.port);
                        auto username = config.getString(replica_name + ".user", config.getString(replica_name + ".username", root_configuration.username));
                        auto password = config.getString(replica_name + ".password", root_configuration.password);

                        connection_info.emplace_back(
                            postgres::PoolWithFailover::AuthSettings{ .database = database, .host = host, .port = static_cast<UInt16>(port), .username = username, .password = password, .priority = priority });
                    }
                }
            }
            else
            {
                connection_info.push_back(std::move(root_configuration));
            }

            dictionary_configuration.emplace(PostgreSQLDictionarySource::Configuration{
                .db = database,
                .schema = config.getString(fmt::format("{}.schema", settings_config_prefix), ""),
                .table = config.getString(fmt::format("{}.table", settings_config_prefix), ""),
                .query = config.getString(fmt::format("{}.query", settings_config_prefix), ""),
                .where = config.getString(fmt::format("{}.where", settings_config_prefix), ""),
                .invalidate_query = config.getString(fmt::format("{}.invalidate_query", settings_config_prefix), ""),
                .update_field = config.getString(fmt::format("{}.update_field", settings_config_prefix), ""),
                .update_lag = config.getUInt64(fmt::format("{}.update_lag", settings_config_prefix), 1)
            });
        }

        auto pool = std::make_shared<postgres::PoolWithFailover>(
                    connection_info,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);

        return std::make_unique<PostgreSQLDictionarySource>(dict_struct, *dictionary_configuration, pool, sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        (void)context;
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `postgresql` is disabled because ClickHouse was built without postgresql support.");
#endif
    };

    factory.registerSource("postgresql", create_table_source);
}

}
