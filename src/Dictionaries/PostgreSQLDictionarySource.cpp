#include "PostgreSQLDictionarySource.h"

#include <Poco/Util/AbstractConfiguration.h>
#include "DictionarySourceFactory.h"
#include "registerDictionaries.h"

#if USE_LIBPQXX
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include "readInvalidateQuery.h"
#include <Interpreters/Context.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

#if USE_LIBPQXX

static const UInt64 max_block_size = 8192;

namespace
{
    ExternalQueryBuilder makeExternalQueryBuilder(const DictionaryStructure & dict_struct, const String & schema, const String & table, const String & where)
    {
        auto schema_value = schema;
        auto table_value = table;

        if (schema_value.empty())
        {
            if (auto pos = table_value.find('.'); pos != std::string::npos)
            {
                schema_value = table_value.substr(0, pos);
                table_value = table_value.substr(pos + 1);
            }
        }
        /// Do not need db because it is already in a connection string.
        return {dict_struct, "", schema_value, table_value, where, IdentifierQuotingStyle::DoubleQuotes};
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
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.where))
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
    , query_builder(makeExternalQueryBuilder(dict_struct, configuration.schema, configuration.table, configuration.where))
    , load_all_query(query_builder.composeLoadAllQuery())
    , update_time(other.update_time)
    , invalidate_query_response(other.invalidate_query_response)
{
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return loadBase(load_all_query);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadUpdatedAll()
{
    auto load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, load_update_query);
    return loadBase(load_update_query);
}

BlockInputStreamPtr PostgreSQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadBase(query);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadBase(query);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadBase(const String & query)
{
    return std::make_shared<PostgreSQLBlockInputStream<>>(pool->get(), query, sample_block, max_block_size);
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
    PostgreSQLBlockInputStream<> block_input_stream(pool->get(), request, invalidate_sample_block, 1);
    return readInvalidateQuery(block_input_stream);
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
    return std::make_unique<PostgreSQLDictionarySource>(*this);
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
        auto pool = std::make_shared<postgres::PoolWithFailover>(
                    config, settings_config_prefix,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);

        PostgreSQLDictionarySource::Configuration configuration
        {
            .db = config.getString(fmt::format("{}.db", settings_config_prefix), ""),
            .schema = config.getString(fmt::format("{}.schema", settings_config_prefix), ""),
            .table = config.getString(fmt::format("{}.table", settings_config_prefix), ""),
            .where = config.getString(fmt::format("{}.where", settings_config_prefix), ""),
            .invalidate_query = config.getString(fmt::format("{}.invalidate_query", settings_config_prefix), ""),
            .update_field = config.getString(fmt::format("{}.update_field", settings_config_prefix), ""),
            .update_lag = config.getUInt64(fmt::format("{}.update_lag", settings_config_prefix), 1)
        };

        return std::make_unique<PostgreSQLDictionarySource>(dict_struct, configuration, pool, sample_block);
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
