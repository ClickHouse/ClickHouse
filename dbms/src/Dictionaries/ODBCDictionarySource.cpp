#include <common/logger_useful.h>
#include <common/LocalDateTime.h>
#include <Poco/Ext/SessionPoolHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Dictionaries/ODBCDictionarySource.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Interpreters/Context.h>
#include <IO/WriteHelpers.h>


namespace DB
{


static const size_t max_block_size = 8192;


ODBCDictionarySource::ODBCDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const Block & sample_block, const Context & context)
    : log(&Logger::get("ODBCDictionarySource")),
    update_time{std::chrono::system_clock::from_time_t(0)},
    dict_struct{dict_struct_},
    db{config.getString(config_prefix + ".db", "")},
    table{config.getString(config_prefix + ".table")},
    where{config.getString(config_prefix + ".where", "")},
    update_field{config.getString(config_prefix + ".update_field", "")},
    sample_block{sample_block},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::None},    /// NOTE Better to obtain quoting style via ODBC interface.
    load_all_query{query_builder.composeLoadAllQuery()},
    invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
{
    std::size_t field_size = context.getSettingsRef().odbc_max_field_size;

    pool = createAndCheckResizePocoSessionPool([&]
    {
        auto session = std::make_shared<Poco::Data::SessionPool>(
            config.getString(config_prefix + ".connector", "ODBC"),
            config.getString(config_prefix + ".connection_string"));

        /// Default POCO value is 1024. Set property manually to make possible reading of longer strings.
        session->setProperty("maxFieldSize", Poco::Any(field_size));
        return session;
    });
}

/// copy-constructor is provided in order to support cloneability
ODBCDictionarySource::ODBCDictionarySource(const ODBCDictionarySource & other)
    : log(&Logger::get("ODBCDictionarySource")),
    update_time{other.update_time},
    dict_struct{other.dict_struct},
    db{other.db},
    table{other.table},
    where{other.where},
    update_field{other.update_field},
    sample_block{other.sample_block},
    pool{other.pool},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::None},
    load_all_query{other.load_all_query},
    invalidate_query{other.invalidate_query}, invalidate_query_response{other.invalidate_query_response}
{
}

std::string ODBCDictionarySource::getUpdateFieldAndDate()
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
        std::string str_time("0000-00-00 00:00:00"); ///for initial load
        return query_builder.composeUpdateQuery(update_field, str_time);
    }
}

BlockInputStreamPtr ODBCDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), load_all_query, sample_block, max_block_size);
}

BlockInputStreamPtr ODBCDictionarySource::loadUpdatedAll()
{
    std::string load_query_update = getUpdateFieldAndDate();

    LOG_TRACE(log, load_query_update);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), load_query_update, sample_block, max_block_size);
}

BlockInputStreamPtr ODBCDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), query, sample_block, max_block_size);
}

BlockInputStreamPtr ODBCDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), query, sample_block, max_block_size);
}

bool ODBCDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool ODBCDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

DictionarySourcePtr ODBCDictionarySource::clone() const
{
    return std::make_unique<ODBCDictionarySource>(*this);
}

std::string ODBCDictionarySource::toString() const
{
    return "ODBC: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

bool ODBCDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
}


std::string ODBCDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block sample_block;
    ColumnPtr column(ColumnString::create());
    sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    ODBCBlockInputStream block_input_stream(pool->get(), request, sample_block, 1);
    return readInvalidateQuery(block_input_stream);
}

}
