#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/writeParenthesisedString.h>
#include <Client/ConnectionPool.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <Interpreters/executeQuery.h>
#include <Common/isLocalAddress.h>
#include <memory>
#include <ext/range.h>
#include <IO/ConnectionTimeouts.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}


static const size_t MAX_CONNECTIONS = 16;

static ConnectionPoolWithFailoverPtr createPool(
        const std::string & host, UInt16 port, bool secure, const std::string & db,
        const std::string & user, const std::string & password, const Context & context)
{
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(context.getSettingsRef());
    ConnectionPoolPtrs pools;
    pools.emplace_back(std::make_shared<ConnectionPool>(
            MAX_CONNECTIONS, host, port, db, user, password, timeouts, "ClickHouseDictionarySource",
            Protocol::Compression::Enable,
            secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));
    return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
}


ClickHouseDictionarySource::ClickHouseDictionarySource(
        const DictionaryStructure & dict_struct_,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const Block & sample_block, Context & context)
    : update_time{std::chrono::system_clock::from_time_t(0)},
        dict_struct{dict_struct_},
        host{config.getString(config_prefix + ".host")},
        port(config.getInt(config_prefix + ".port")),
        secure(config.getBool(config_prefix + ".secure", false)),
        user{config.getString(config_prefix + ".user", "")},
        password{config.getString(config_prefix + ".password", "")},
        db{config.getString(config_prefix + ".db", "")},
        table{config.getString(config_prefix + ".table")},
        where{config.getString(config_prefix + ".where", "")},
        update_field{config.getString(config_prefix + ".update_field", "")},
        query_builder{dict_struct, db, table, where, ExternalQueryBuilder::Backticks},
        sample_block{sample_block}, context(context),
        is_local{isLocalAddress({ host, port }, config.getInt("tcp_port", 0))},
        pool{is_local ? nullptr : createPool(host, port, secure, db, user, password, context)},
        load_all_query{query_builder.composeLoadAllQuery()}
{}


ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time},
        dict_struct{other.dict_struct},
        host{other.host}, port{other.port},
        secure{other.secure},
        user{other.user}, password{other.password},
        db{other.db}, table{other.table},
        where{other.where},
        update_field{other.update_field},
        query_builder{dict_struct, db, table, where, ExternalQueryBuilder::Backticks},
        sample_block{other.sample_block}, context(other.context),
        is_local{other.is_local},
        pool{is_local ? nullptr : createPool(host, port, secure, db, user, password, context)},
        load_all_query{other.load_all_query}
{}

std::string ClickHouseDictionarySource::getUpdateFieldAndDate()
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

BlockInputStreamPtr ClickHouseDictionarySource::loadAll()
{
    /** Query to local ClickHouse is marked internal in order to avoid
      *    the necessity of holding process_list_element shared pointer.
      */
    if (is_local)
        return executeQuery(load_all_query, context, true).in;
    return std::make_shared<RemoteBlockInputStream>(pool, load_all_query, sample_block, context);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadUpdatedAll()
{
    std::string load_update_query = getUpdateFieldAndDate();
    if (is_local)
        return executeQuery(load_update_query, context, true).in;
    return std::make_shared<RemoteBlockInputStream>(pool, load_update_query, sample_block, context);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    return createStreamForSelectiveLoad(
        query_builder.composeLoadIdsQuery(ids));
}


BlockInputStreamPtr ClickHouseDictionarySource::loadKeys(
    const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    return createStreamForSelectiveLoad(
        query_builder.composeLoadKeysQuery(
            key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES));
}

bool ClickHouseDictionarySource::hasUpdateField() const
{
    return !update_field.empty();
}

std::string ClickHouseDictionarySource::toString() const
{
    return "ClickHouse: " + db + '.' + table + (where.empty() ? "" : ", where: " + where);
}


BlockInputStreamPtr ClickHouseDictionarySource::createStreamForSelectiveLoad(const std::string & query)
{
    if (is_local)
        return executeQuery(query, context, true).in;
    return std::make_shared<RemoteBlockInputStream>(pool, query, sample_block, context);
}

}
