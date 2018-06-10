#include <Common/config.h>
#if USE_MYSQL

#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <common/logger_useful.h>
#include <common/LocalDateTime.h>

#include <Dictionaries/MySQLDictionarySource.h>
#include <Dictionaries/MySQLBlockInputStream.h>
#include <Dictionaries/readInvalidateQuery.h>

#include <IO/WriteHelpers.h>


namespace DB
{

static const size_t max_block_size = 8192;


MySQLDictionarySource::MySQLDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const Block & sample_block)
    : log(&Logger::get("MySQLDictionarySource")),
    update_time{std::chrono::system_clock::from_time_t(0)},
    dict_struct{dict_struct_},
    db{config.getString(config_prefix + ".db", "")},
    table{config.getString(config_prefix + ".table")},
    where{config.getString(config_prefix + ".where", "")},
    update_field{config.getString(config_prefix + ".update_field", "")},
    dont_check_update_time{config.getBool(config_prefix + ".dont_check_update_time", false)},
    sample_block{sample_block},
    pool{config, config_prefix},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::Backticks},
    load_all_query{query_builder.composeLoadAllQuery()},
    invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
    : log(&Logger::get("MySQLDictionarySource")),
    update_time{other.update_time},
    dict_struct{other.dict_struct},
    db{other.db},
    table{other.table},
    where{other.where},
    update_field{other.update_field},
    dont_check_update_time{other.dont_check_update_time},
    sample_block{other.sample_block},
    pool{other.pool},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::Backticks},
    load_all_query{other.load_all_query}, last_modification{other.last_modification},
    invalidate_query{other.invalidate_query}, invalidate_query_response{other.invalidate_query_response}
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
        std::string str_time("0000-00-00 00:00:00"); ///for initial load
        return query_builder.composeUpdateQuery(update_field, str_time);
    }
}

BlockInputStreamPtr MySQLDictionarySource::loadAll()
{
    last_modification = getLastModification();

    LOG_TRACE(log, load_all_query);
    return std::make_shared<MySQLBlockInputStream>(pool.Get(), load_all_query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadUpdatedAll()
{
    last_modification = getLastModification();

    std::string load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, load_update_query);
    return std::make_shared<MySQLBlockInputStream>(pool.Get(), load_update_query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.

    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<MySQLBlockInputStream>(pool.Get(), query, sample_block, max_block_size);
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

    return getLastModification() > last_modification;
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

LocalDateTime MySQLDictionarySource::getLastModification() const
{
    LocalDateTime update_time{std::time(nullptr)};

    if (dont_check_update_time)
        return update_time;

    try
    {
        auto connection = pool.Get();
        auto query = connection->query("SHOW TABLE STATUS LIKE " + quoteForLike(table));

        LOG_TRACE(log, query.str());

        auto result = query.use();

        size_t fetched_rows = 0;
        if (auto row = result.fetch())
        {
            ++fetched_rows;
            const auto UPDATE_TIME_IDX = 12;
            const auto & update_time_value = row[UPDATE_TIME_IDX];

            if (!update_time_value.isNull())
            {
                update_time = update_time_value.getDateTime();
                LOG_TRACE(log, "Got update time: " << update_time);
            }

            /// fetch remaining rows to avoid "commands out of sync" error
            while (result.fetch())
                ++fetched_rows;
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
    return update_time;
}

std::string MySQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block sample_block;
    ColumnPtr column(ColumnString::create());
    sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    MySQLBlockInputStream block_input_stream(pool.Get(), request, sample_block, 1);
    return readInvalidateQuery(block_input_stream);
}

}

#endif
