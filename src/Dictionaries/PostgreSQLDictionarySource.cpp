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

static const std::unordered_set<std::string_view> dictionary_allowed_keys = {
    "host", "port", "user", "password", "db", "database", "table", "schema",
    "update_field", "update_lag", "invalidate_query", "query", "where", "name", "priority"};

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
        auto has_config_key = [](const String & key) { return dictionary_allowed_keys.contains(key) || key.starts_with("replica"); };
        auto configuration = getExternalDataSourceConfigurationByPriority(config, settings_config_prefix, context, has_config_key);
        const auto & settings = context->getSettingsRef();
        auto pool = std::make_shared<postgres::PoolWithFailover>(
            configuration.replicas_configurations,
            settings.postgresql_connection_pool_size,
            settings.postgresql_connection_pool_wait_timeout,
            POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
            settings.postgresql_connection_pool_auto_close_connection);

        PostgreSQLDictionarySource::Configuration dictionary_configuration
        {
            .db = configuration.database,
            .schema = configuration.schema,
            .table = configuration.table,
            .query = config.getString(fmt::format("{}.query", settings_config_prefix), ""),
            .where = config.getString(fmt::format("{}.where", settings_config_prefix), ""),
            .invalidate_query = config.getString(fmt::format("{}.invalidate_query", settings_config_prefix), ""),
            .update_field = config.getString(fmt::format("{}.update_field", settings_config_prefix), ""),
            .update_lag = config.getUInt64(fmt::format("{}.update_lag", settings_config_prefix), 1)
        };

        return std::make_unique<PostgreSQLDictionarySource>(dict_struct, dictionary_configuration, pool, sample_block);
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
