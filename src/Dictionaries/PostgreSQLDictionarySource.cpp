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
    ExternalQueryBuilder makeExternalQueryBuilder(const DictionaryStructure & dict_struct, String & schema, String & table, const String & where)
    {
        if (schema.empty())
        {
            if (auto pos = table.find('.'); pos != std::string::npos)
            {
                schema = table.substr(0, pos);
                table = table.substr(pos + 1);
            }
        }
        /// Do not need db because it is already in a connection string.
        return {dict_struct, "", schema, table, where, IdentifierQuotingStyle::DoubleQuotes};
    }
}


PostgreSQLDictionarySource::PostgreSQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    postgres::PoolWithFailoverPtr pool_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::string & config_prefix,
    const Block & sample_block_)
    : dict_struct{dict_struct_}
    , sample_block(sample_block_)
    , pool(std::move(pool_))
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , db(config_.getString(fmt::format("{}.db", config_prefix), ""))
    , schema(config_.getString(fmt::format("{}.schema", config_prefix), ""))
    , table(config_.getString(fmt::format("{}.table", config_prefix), ""))
    , where(config_.getString(fmt::format("{}.where", config_prefix), ""))
    , query_builder(makeExternalQueryBuilder(dict_struct, schema, table, where))
    , load_all_query(query_builder.composeLoadAllQuery())
    , invalidate_query(config_.getString(fmt::format("{}.invalidate_query", config_prefix), ""))
    , update_field(config_.getString(fmt::format("{}.update_field", config_prefix), ""))
{
}


/// copy-constructor is provided in order to support cloneability
PostgreSQLDictionarySource::PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other)
    : dict_struct(other.dict_struct)
    , sample_block(other.sample_block)
    , pool(other.pool)
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , db(other.db)
    , table(other.table)
    , where(other.where)
    , query_builder(dict_struct, "", "", table, where, IdentifierQuotingStyle::DoubleQuotes)
    , load_all_query(query_builder.composeLoadAllQuery())
    , invalidate_query(other.invalidate_query)
    , update_time(other.update_time)
    , update_field(other.update_field)
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
    return std::make_shared<PostgreSQLBlockInputStream>(pool->get(), query, sample_block, max_block_size);
}

bool PostgreSQLDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
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
    PostgreSQLBlockInputStream block_input_stream(pool->get(), request, invalidate_sample_block, 1);
    return readInvalidateQuery(block_input_stream);
}


bool PostgreSQLDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}


std::string PostgreSQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - 1;
        std::string str_time = DateLUT::instance().timeToString(hr_time);
        update_time = std::chrono::system_clock::now();
        return query_builder.composeUpdateQuery(update_field, str_time);
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
    return "PostgreSQL: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

#endif

void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & root_config_prefix,
                                 Block & sample_block,
                                 ContextPtr context,
                                 const std::string & /* default_database */,
                                 bool /* check_config */) -> DictionarySourcePtr
    {
#if USE_LIBPQXX
        const auto config_prefix = root_config_prefix + ".postgresql";
        auto pool = std::make_shared<postgres::PoolWithFailover>(
                    config, config_prefix,
                    context->getSettingsRef().postgresql_connection_pool_size,
                    context->getSettingsRef().postgresql_connection_pool_wait_timeout);
        return std::make_unique<PostgreSQLDictionarySource>(
                dict_struct, pool, config, config_prefix, sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)root_config_prefix;
        (void)sample_block;
        (void)context;
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `postgresql` is disabled because ClickHouse was built without postgresql support.");
#endif
    };
    factory.registerSource("postgresql", create_table_source);
}

}
