#include "MySQLDictionarySource.h"

#include <Poco/Util/AbstractConfiguration.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "registerDictionaries.h"
#include <mysqlxx/PoolFactory.h>

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
                                 ContextPtr /* context */,
                                 const std::string & /* default_database */,
                                 bool /* check_config */) -> DictionarySourcePtr {
#if USE_MYSQL
    std::string source_prefix = config_prefix + ".mysql";

    MySQLDictionarySource::Configuration configuration
    {
        .db = config.getString(source_prefix + ".db", ""),
        .table = config.getString(source_prefix + ".table"),
        .where = config.getString(source_prefix + ".where", ""),
        .update_field = config.getString(source_prefix + ".update_field", ""),
        .invalidate_query = config.getString(source_prefix + ".invalidate_query", ""),
        .dont_check_update_time = config.getBool(source_prefix + ".dont_check_update_time", false),
    };

    auto pool = mysqlxx::PoolFactory::instance().getPoolWithFailover(config, source_prefix);

    return std::make_unique<MySQLDictionarySource>(dict_struct, configuration, pool, sample_block);
#else
        (void)dict_struct;
        (void)config;
        (void)config_prefix;
        (void)sample_block;
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
            "Dictionary source of type `mysql` is disabled because ClickHouse was built without mysql support.");
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
#    include <mysqlxx/Exception.h>
#    include <mysqlxx/PoolFactory.h>

namespace DB
{

static const UInt64 max_block_size = 8192;

MySQLDictionarySource::MySQLDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    mysqlxx::PoolPtr pool_,
    const Block & sample_block_)
    : log(&Poco::Logger::get("MySQLDictionarySource"))
    , update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration(configuration_)
    , pool(std::move(pool_))
    , sample_block{sample_block_}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
}

/// copy-constructor is provided in order to support cloneability
MySQLDictionarySource::MySQLDictionarySource(const MySQLDictionarySource & other)
    : log(&Poco::Logger::get("MySQLDictionarySource"))
    , update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , pool{other.pool}
    , sample_block{other.sample_block}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , load_all_query{other.load_all_query}
    , last_modification{other.last_modification}
    , invalidate_query_response{other.invalidate_query_response}
{
}

std::string MySQLDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - 1;
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

BlockInputStreamPtr MySQLDictionarySource::loadFromQuery(const String & query)
{
    return std::make_shared<MySQLLazyBlockInputStream>(pool, query, sample_block, max_block_size);
}

BlockInputStreamPtr MySQLDictionarySource::loadAll()
{
    std::cerr << "MySQLDictionarySource::loadAll" << std::endl;

    auto connection = pool->getEntry();
    last_modification = getLastModification(connection);

    LOG_TRACE(log, load_all_query);
    return loadFromQuery(load_all_query);
}

BlockInputStreamPtr MySQLDictionarySource::loadUpdatedAll()
{
    std::cerr << "MySQLDictionarySource::loadUpdatedAll" << std::endl;

    auto connection = pool->getEntry();
    last_modification = getLastModification(connection);

    std::string load_update_query = getUpdateFieldAndDate();
    LOG_TRACE(log, load_update_query);
    return loadFromQuery(load_update_query);
}

BlockInputStreamPtr MySQLDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return loadFromQuery(query);
}

BlockInputStreamPtr MySQLDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    /// We do not log in here and do not update the modification time, as the request can be large, and often called.
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return loadFromQuery(query);
}

bool MySQLDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        if (response == invalidate_query_response)
            return false;
        invalidate_query_response = response;
        return true;
    }

    if (configuration.dont_check_update_time)
        return true;
    auto connection = pool->getEntry();
    return getLastModification(connection) > last_modification;
}

bool MySQLDictionarySource::supportsSelectiveLoad() const
{
    return true;
}

bool MySQLDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

DictionarySourcePtr MySQLDictionarySource::clone() const
{
    return std::make_unique<MySQLDictionarySource>(*this);
}

std::string MySQLDictionarySource::toString() const
{
    const std::string & where = configuration.where;
    return "MySQL: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
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

LocalDateTime MySQLDictionarySource::getLastModification(mysqlxx::IPool::Entry & connection) const
{
    LocalDateTime modification_time{std::time(nullptr)};

    if (configuration.dont_check_update_time)
        return modification_time;

    try
    {
        auto query = connection->query("SHOW TABLE STATUS LIKE " + quoteForLike(configuration.table));

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
                LOG_TRACE(log, "Got modification time: {}", update_time_value.getString());
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
    return modification_time;
}

std::string MySQLDictionarySource::doInvalidateQuery(const std::string & request) const
{
    Block invalidate_sample_block;
    ColumnPtr column(ColumnString::create());
    invalidate_sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    MySQLBlockInputStream block_input_stream(pool->getEntry(), request, invalidate_sample_block, 1);
    return readInvalidateQuery(block_input_stream);
}

}

#endif
