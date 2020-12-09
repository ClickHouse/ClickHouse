#include "PostgreSQLDictionarySource.h"

#if USE_LIBPQXX

#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <common/LocalDateTime.h>
#include <DataStreams/PostgreSQLBlockInputStream.h>
#include "readInvalidateQuery.h"
#include "DictionarySourceFactory.h"

namespace DB
{
static const UInt64 max_block_size = 8192;


PostgreSQLDictionarySource::PostgreSQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config_,
    const std::string & config_prefix,
    const std::string & connection_str,
    const Block & sample_block_)
    : dict_struct{dict_struct_}
    , sample_block(sample_block_)
    , connection(std::make_shared<pqxx::connection>(connection_str))
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , db(config_.getString(fmt::format("{}.db", config_prefix), ""))
    , table(config_.getString(fmt::format("{}.table", config_prefix), ""))
    , where(config_.getString(fmt::format("{}.where", config_prefix), ""))
    , query_builder(dict_struct, "", "", table, where, IdentifierQuotingStyle::DoubleQuotes)
    , load_all_query(query_builder.composeLoadAllQuery())
    , invalidate_query(config_.getString(fmt::format("{}.invalidate_query", config_prefix), ""))
{
}


/// copy-constructor is provided in order to support cloneability
PostgreSQLDictionarySource::PostgreSQLDictionarySource(const PostgreSQLDictionarySource & other)
    : dict_struct(other.dict_struct)
    , sample_block(other.sample_block)
    , connection(other.connection)
    , log(&Poco::Logger::get("PostgreSQLDictionarySource"))
    , db(other.db)
    , table(other.table)
    , where(other.where)
    , query_builder(dict_struct, "", "", table, where, IdentifierQuotingStyle::DoubleQuotes)
    , load_all_query(query_builder.composeLoadAllQuery())
    , invalidate_query(other.invalidate_query)
    , update_time(other.update_time)
    , invalidate_query_response(other.invalidate_query_response)
{
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return std::make_shared<PostgreSQLBlockInputStream>(
            connection, load_all_query, sample_block, max_block_size);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadUpdatedAll()
{
    auto load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, load_update_query);
    return std::make_shared<PostgreSQLBlockInputStream>(connection, load_update_query, sample_block, max_block_size);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<PostgreSQLBlockInputStream>(connection, query, sample_block, max_block_size);
}


BlockInputStreamPtr PostgreSQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<PostgreSQLBlockInputStream>(connection, query, sample_block, max_block_size);
}


bool PostgreSQLDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
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
    PostgreSQLBlockInputStream block_input_stream(connection, request, invalidate_sample_block, 1);
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
        auto tmp_time = update_time;
        update_time = std::chrono::system_clock::now();
        time_t hr_time = std::chrono::system_clock::to_time_t(tmp_time) - 1;
        std::string str_time = std::to_string(LocalDateTime(hr_time));
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


void registerDictionarySourcePostgreSQL(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & root_config_prefix,
                                 Block & sample_block,
                                 const Context & /* context */,
                                 const std::string & /* default_database */,
                                 bool /* check_config */) -> DictionarySourcePtr
    {
        const auto config_prefix = root_config_prefix + ".postgresql";
        auto connection_str = fmt::format("dbname={} host={} port={} user={} password={}",
            config.getString(fmt::format("{}.db", config_prefix), ""),
            config.getString(fmt::format("{}.host", config_prefix), ""),
            config.getUInt(fmt::format("{}.port", config_prefix), 0),
            config.getString(fmt::format("{}.user", config_prefix), ""),
            config.getString(fmt::format("{}.password", config_prefix), ""));

        return std::make_unique<PostgreSQLDictionarySource>(
                dict_struct, config, config_prefix, connection_str, sample_block);
    };
    factory.registerSource("postgresql", create_table_source);
}
}

#endif
