#include <Dictionaries/PostgreSQLDictionarySource.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Core/QualifiedTableName.h>
#include <Core/Settings.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Dictionaries/registerDictionaries.h>

#if USE_LIBPQXX
#include <Columns/ColumnString.h>
#include <Common/DateLUTImpl.h>
#include <Common/RemoteHostFilter.h>
#include <DataTypes/DataTypeString.h>
#include <Processors/Sources/PostgreSQLSource.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Common/logger_useful.h>
#endif


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
}

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int BAD_ARGUMENTS;
}

static const ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> dictionary_allowed_keys = {
    "host", "port", "user", "password", "db", "database", "table", "schema", "background_reconnect",
    "update_field", "update_lag", "invalidate_query", "query", "where", "name", "priority"};

#if USE_LIBPQXX

static const UInt64 max_block_size = 8192;

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
    , log(getLogger("PostgreSQLDictionarySource"))
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
    , log(getLogger("PostgreSQLDictionarySource"))
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.query, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
    , update_time(other.update_time)
    , invalidate_query_response(other.invalidate_query_response)
{
}


QueryPipeline PostgreSQLDictionarySource::loadAll()
{
    LOG_TRACE(log, fmt::runtime(load_all_query));
    return loadBase(load_all_query);
}


QueryPipeline PostgreSQLDictionarySource::loadUpdatedAll()
{
    auto load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, fmt::runtime(load_update_query));
    return loadBase(load_update_query);
}

QueryPipeline PostgreSQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadBase(query);
}


QueryPipeline PostgreSQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadBase(query);
}


QueryPipeline PostgreSQLDictionarySource::loadBase(const String & query)
{
    return QueryPipeline(std::make_shared<PostgreSQLSource<>>(pool->get(), query, sample_block, max_block_size));
}


bool PostgreSQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        if (response == invalidate_query_response)
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

    update_time = std::chrono::system_clock::now();
    return query_builder.composeLoadAllQuery();
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

static void validateConfigKeys(
    const Poco::Util::AbstractConfiguration & dict_config, const String & config_prefix)
{
    Poco::Util::AbstractConfiguration::Keys config_keys;
    dict_config.keys(config_prefix, config_keys);
    for (const auto & config_key : config_keys)
    {
        if (dictionary_allowed_keys.contains(config_key) || startsWith(config_key, "replica"))
            continue;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected key `{}` in dictionary source configuration", config_key);
    }
}

#endif

void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 [[maybe_unused]] bool created_from_ddl) -> DictionarySourcePtr
    {
#if USE_LIBPQXX
        const auto settings_config_prefix = config_prefix + ".postgresql";
        const auto & settings = context->getSettingsRef();

        std::optional<PostgreSQLDictionarySource::Configuration> dictionary_configuration;
        postgres::PoolWithFailover::ReplicasConfigurationByPriority replicas_by_priority;

        bool bg_reconnect = false;

        auto named_collection = created_from_ddl ? tryGetNamedCollectionWithOverrides(config, settings_config_prefix, context) : nullptr;
        if (named_collection)
        {
            validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(*named_collection, {}, dictionary_allowed_keys);

            StoragePostgreSQL::Configuration common_configuration;
            common_configuration.host = named_collection->getOrDefault<String>("host", "");
            common_configuration.port = named_collection->getOrDefault<UInt64>("port", 0);
            common_configuration.username = named_collection->getOrDefault<String>("user", "");
            common_configuration.password = named_collection->getOrDefault<String>("password", "");
            common_configuration.database = named_collection->getAnyOrDefault<String>({"database", "db"}, "");
            common_configuration.schema = named_collection->getOrDefault<String>("schema", "");
            common_configuration.table = named_collection->getOrDefault<String>("table", "");

            dictionary_configuration.emplace(PostgreSQLDictionarySource::Configuration{
                .db = common_configuration.database,
                .schema = common_configuration.schema,
                .table = common_configuration.table,
                .query = named_collection->getOrDefault<String>("query", ""),
                .where = named_collection->getOrDefault<String>("where", ""),
                .invalidate_query = named_collection->getOrDefault<String>("invalidate_query", ""),
                .update_field = named_collection->getOrDefault<String>("update_field", ""),
                .update_lag = named_collection->getOrDefault<UInt64>("update_lag", 1),
            });

            bg_reconnect = named_collection->getOrDefault<bool>("background_reconnect", false);

            replicas_by_priority[0].emplace_back(common_configuration);
        }
        else
        {
            validateConfigKeys(config, settings_config_prefix);

            StoragePostgreSQL::Configuration common_configuration;
            common_configuration.host = config.getString(settings_config_prefix + ".host", "");
            common_configuration.port = config.getUInt(settings_config_prefix + ".port", 0);
            common_configuration.username = config.getString(settings_config_prefix + ".user", "");
            common_configuration.password = config.getString(settings_config_prefix + ".password", "");
            common_configuration.database = config.getString(fmt::format("{}.database", settings_config_prefix), config.getString(fmt::format("{}.db", settings_config_prefix), ""));
            common_configuration.schema = config.getString(fmt::format("{}.schema", settings_config_prefix), "");
            common_configuration.table = config.getString(fmt::format("{}.table", settings_config_prefix), "");

            dictionary_configuration.emplace(PostgreSQLDictionarySource::Configuration
            {
                .db = common_configuration.database,
                .schema = common_configuration.schema,
                .table = common_configuration.table,
                .query = config.getString(fmt::format("{}.query", settings_config_prefix), ""),
                .where = config.getString(fmt::format("{}.where", settings_config_prefix), ""),
                .invalidate_query = config.getString(fmt::format("{}.invalidate_query", settings_config_prefix), ""),
                .update_field = config.getString(fmt::format("{}.update_field", settings_config_prefix), ""),
                .update_lag = config.getUInt64(fmt::format("{}.update_lag", settings_config_prefix), 1)
            });

            bg_reconnect = config.getBool(fmt::format("{}.background_reconnect", settings_config_prefix), false);

            if (config.has(settings_config_prefix + ".replica"))
            {
                Poco::Util::AbstractConfiguration::Keys config_keys;
                config.keys(settings_config_prefix, config_keys);

                for (const auto & config_key : config_keys)
                {
                    if (config_key.starts_with("replica"))
                    {
                        String replica_name = settings_config_prefix + "." + config_key;
                        StoragePostgreSQL::Configuration replica_configuration{common_configuration};

                        size_t priority = config.getInt(replica_name + ".priority", 0);
                        replica_configuration.host = config.getString(replica_name + ".host", common_configuration.host);
                        replica_configuration.port = config.getUInt(replica_name + ".port", common_configuration.port);
                        replica_configuration.username = config.getString(replica_name + ".user", common_configuration.username);
                        replica_configuration.password = config.getString(replica_name + ".password", common_configuration.password);

                        if (replica_configuration.host.empty() || replica_configuration.port == 0
                            || replica_configuration.username.empty() || replica_configuration.password.empty())
                        {
                            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                            "Named collection of connection parameters is missing some "
                                            "of the parameters and no other dictionary parameters are added");
                        }

                        replicas_by_priority[priority].emplace_back(replica_configuration);
                    }
                }
            }
            else
            {
                replicas_by_priority[0].emplace_back(common_configuration);
            }
        }
        if (created_from_ddl)
        {
            for (const auto & [_, replicas] : replicas_by_priority)
                for (const auto & replica : replicas)
                    context->getRemoteHostFilter().checkHostAndPort(replica.host, toString(replica.port));
        }


        auto pool = std::make_shared<postgres::PoolWithFailover>(
            replicas_by_priority,
            settings[Setting::postgresql_connection_pool_size],
            settings[Setting::postgresql_connection_pool_wait_timeout],
            settings[Setting::postgresql_connection_pool_retries],
            settings[Setting::postgresql_connection_pool_auto_close_connection],
            settings[Setting::postgresql_connection_attempt_timeout],
            bg_reconnect);


        return std::make_unique<PostgreSQLDictionarySource>(dict_struct, dictionary_configuration.value(), pool, sample_block);
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
