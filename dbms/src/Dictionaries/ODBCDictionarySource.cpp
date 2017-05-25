#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Poco/Data/SessionPool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Dictionaries/ODBCDictionarySource.h>
#include <Dictionaries/ODBCBlockInputStream.h>
#include <common/logger_useful.h>
#include <Dictionaries/readInvalidateQuery.h>


namespace DB
{


static const size_t max_block_size = 8192;


ODBCDictionarySource::ODBCDictionarySource(const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix,
    const Block & sample_block)
    : log(&Logger::get("ODBCDictionarySource")),
    dict_struct{dict_struct_},
    db{config.getString(config_prefix + ".db", "")},
    table{config.getString(config_prefix + ".table")},
    where{config.getString(config_prefix + ".where", "")},
    sample_block{sample_block},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::None},    /// NOTE Better to obtain quoting style via ODBC interface.
    load_all_query{query_builder.composeLoadAllQuery()},
    invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
{
    pool = createAndCheckResizePocoSessionPool([&] () { return std::make_shared<Poco::Data::SessionPool>(
        config.getString(config_prefix + ".connector", "ODBC"),
        config.getString(config_prefix + ".connection_string"));
    });
}

/// copy-constructor is provided in order to support cloneability
ODBCDictionarySource::ODBCDictionarySource(const ODBCDictionarySource & other)
    : log(&Logger::get("ODBCDictionarySource")),
    dict_struct{other.dict_struct},
    db{other.db},
    table{other.table},
    where{other.where},
    sample_block{other.sample_block},
    pool{other.pool},
    query_builder{dict_struct, db, table, where, ExternalQueryBuilder::None},
    load_all_query{other.load_all_query},
    invalidate_query{other.invalidate_query}, invalidate_query_response{other.invalidate_query_response}
{
}

std::shared_ptr<Poco::Data::SessionPool> ODBCDictionarySource::createAndCheckResizePocoSessionPool(PocoSessionPoolConstructor pool_constr)
{
    static std::mutex mutex;

    Poco::ThreadPool & pool = Poco::ThreadPool::defaultPool();

    /// NOTE: The lock don't guarantee that external users of the pool don't change its capacity
    std::unique_lock<std::mutex> lock(mutex);

    if (pool.available() == 0)
        pool.addCapacity(2 * std::max(pool.capacity(), 1));

    return pool_constr();
}

BlockInputStreamPtr ODBCDictionarySource::loadAll()
{
    LOG_TRACE(log, load_all_query);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), load_all_query, sample_block, max_block_size);
}

BlockInputStreamPtr ODBCDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    const auto query = query_builder.composeLoadIdsQuery(ids);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), query, sample_block, max_block_size);
}

BlockInputStreamPtr ODBCDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<std::size_t> & requested_rows)
{
    const auto query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::AND_OR_CHAIN);
    return std::make_shared<ODBCBlockInputStream>(pool->get(), query, sample_block, max_block_size);
}

bool ODBCDictionarySource::supportsSelectiveLoad() const
{
    return true;
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
    ColumnPtr column(std::make_shared<ColumnString>());
    sample_block.insert(ColumnWithTypeAndName(column, std::make_shared<DataTypeString>(), "Sample Block"));
    ODBCBlockInputStream block_input_stream(pool->get(), request, sample_block, 1);
    return readInvalidateQuery(block_input_stream);
}

}
