#include "MySQLDictionarySource.h"

#include <Poco/Util/AbstractConfiguration.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

void registerDictionarySourceMysql(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & /* context */,
                                 bool /* check_config */) -> DictionarySourcePtr {
#if USE_MYSQL
        return std::make_unique<MySQLDictionarySource>(dict_struct, config, config_prefix + ".mysql", sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception{"Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    };
    factory.registerSource("mysql", create_table_source);
}

}


#if USE_MYSQL
#    include <Columns/ColumnString.h>
#    include <DataTypes/DataTypeString.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <common/LocalDateTime.h>
#    include <common/logger_useful.h>
#    include <Formats/MySQLBlockInputStream.h>
#    include "readInvalidateQuery.h"
#    include <mysqlxx/PoolFactory.h>

namespace DB
{
static const UInt64 max_block_size = 8192;


MySQLDictionarySource::MySQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const Block & sample_block_)
    : log(&Poco::Logger::get("MySQLDictionarySource"))
    , update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , db{config.getString(config_prefix + ".db", "")}
    , table{config.getString(config_prefix + ".table")}
    , where{config.getString(config_prefix + ".where", "")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , dont_check_update_time{config.getBool(config_prefix + ".dont_check_update_time", false)}
    , sample_block{sample_block_}
    , pool{mysqlxx::PoolFactory::instance().get(config, config_prefix)}
    , query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::Backticks}
    , load_all_query{query_builder.composeLoadAllQuery()}
    , invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
    , close_connection{config.getBool(config_prefix + ".close_connection", false) || config.getBool(config_prefix + ".share_connection", false)}
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
    : log(&Poco::Logger::get("MySQLDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , db{other.db}
    , table{other.table}
    , where{other.where}
    , update_field{other.update_field}
    , dont_check_update_time{other.dont_check_update_time}
    , sample_block{other.sample_block}
    , pool{other.pool}
    , query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::Backticks}
    , load_all_query{other.load_all_query}
    , last_modification{other.last_modification}
    , invalidate_query{other.invalidate_query}
    , invalidate_query_response{other.invalidate_query_response}
    , close_connection{other.close_connection}
{
}

std::string MySQLDictionarySource::getUpdateFieldAndDate()
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

BlockInputStreamPtr MySQLDictionarySource::loadAll()
{
    auto connection = pool.get();
    last_modification = getLastModification(connection, false);

    LOG_TRACE(log, load_all_query);
    return std::make_shared<MySQLBlockInputStream>(connection, load_all_query, sample_block, max_block_size, close_connection);
}

BlockInputStreamPtr MySQLDictionarySource::loadUpdatedAll()
{
    auto connection = pool.get();
    last_modification = getLastModification(connection, false);

    std::string load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, load_update_query);
    return std::make_shared<MySQLBlockInputStream>(connection, load_update_query, sample_block, max_block_size, close_connection);
}

BlockInputStreamPtr MySQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<MySQLBlockInputStream>(pool.get(), query, sample_block, max_block_size, close_connection);
}

BlockInputStreamPtr MySQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<MySQLBlockInputStream>(pool.get(), query, sample_block, max_block_size, close_connection);
}

bool MySQLDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
        if (response == invalidate_query_response)
            return false;
        invalidate_query_response = response;
        return true;
    }

    if (dont_check_update_time)
        return true;
    auto connection = pool.get();
    return getLastModification(connection, true) > last_modification;
}

bool MySQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool MySQLDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

DictionarySourcePtr MySQLDictionarySource::clone() const
{
    return std::make_unique<MySQLDictionarySource>(*this);
}

std::string MySQLDictionarySource::toString() const
{
    return "MySQL: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}

std::string MySQLDictionarySource::quoteForLike(const std::string s)
{
    std::string tmp;
    tmp.reserve(s.size());

    for (auto c : s)
    {
        if (c == '%' || c == '_' || c == '\\')
            tmp.push_back('\\');
        tmp.push_back(c);
    }

    WriteBufferFromOwnString out;
    writeQuoted(tmp, out);
    return out.str();
}

LocalDateTime MySQLDictionarySource::getLastModification(mysqlxx::Pool::Entry & connection, bool allow_connection_closure) const
{
    LocalDateTime modification_time{std::time(nullptr)};

    if (dont_check_update_time)
        return modification_time;

    try
    {
        auto query = connection->query("SHOW TABLE STATUS LIKE " + quoteForLike(table));

        LOG_TRACE(log, query.str());

        auto result = query.use();

        size_t fetched_rows = 0;
        if (auto row = result.fetch())
        {
            ++fetched_rows;
            static const auto UPDATE_TIME_IDX = 12;
            const auto & update_time_value = row[UPDATE_TIME_IDX];

            if (!update_time_value.isNull())
            {
                modification_time = update_time_value.getDateTime();
                LOG_TRACE(log, "Got modification time: {}", modification_time);
            }

            /// fetch remaining rows to avoid "commands out of sync" error
            while (result.fetch())
                ++fetched_rows;
        }

        if (close_connection && allow_connection_closure)
        {
            connection.disconnect();
        }

        if (0 == fetched_rows)
            LOG_ERROR(log, "Cannot find table in SHOW TABLE STATUS result.");

        if (fetched_rows > 1)
            LOG_ERROR(log, "Found more than one table in SHOW TABLE STATUS result.");
    }
    catch (...)
    {
        tryLogCurrentException("MySQLDictionarySource");
    }
    /// we suppose failure to get modification time is not an error, therefore return current time
    return modification_time;
}

std::string MySQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    MySQLBlockInputStream block_input_stream(pool.get(), request, invalidate_sample_block, 1, close_connection);
    return readInvalidateQuery(block_input_stream);
}

}

#endif
