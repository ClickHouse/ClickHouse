#include "ClickHouseDictionarySource.h"
#include <memory>
#include <Client/ConnectionPool.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/executeQuery.h>
#include <Common/isLocalAddress.h>
#include <ext/range.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include "readInvalidateQuery.h"
#include "writeParenthesisedString.h"


namespace DB
{
namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}


static const size_t MAX_CONNECTIONS = 16;

static ConnectionPoolWithFailoverPtr createPool(
    const std::string & host,
    UInt16 port,
    bool secure,
    const std::string & db,
    const std::string & user,
    const std::string & password,
    const Context & context)
{
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(context.getSettingsRef());
    ConnectionPoolPtrs pools;
    pools.emplace_back(std::make_shared<ConnectionPool>(
        MAX_CONNECTIONS,
        host,
        port,
        db,
        user,
        password,
        timeouts,
        "ClickHouseDictionarySource",
        Protocol::Compression::Enable,
        secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));
    return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
}


ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const Block & sample_block,
    Context & context_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , host{config.getString(config_prefix + ".host")}
    , port(config.getInt(config_prefix + ".port"))
    , secure(config.getBool(config_prefix + ".secure", false))
    , user{config.getString(config_prefix + ".user", "")}
    , password{config.getString(config_prefix + ".password", "")}
    , db{config.getString(config_prefix + ".db", "")}
    , table{config.getString(config_prefix + ".table")}
    , where{config.getString(config_prefix + ".where", "")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
    , query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block}
    , context(context_)
    , is_local{isLocalAddress({host, port}, context.getTCPPort())}
    , pool{is_local ? nullptr : createPool(host, port, secure, db, user, password, context)}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
    /// We should set user info even for the case when the dictionary is loaded in-process (without TCP communication).
    context.setUser(user, password, Poco::Net::SocketAddress("127.0.0.1", 0), {});
}


ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , host{other.host}
    , port{other.port}
    , secure{other.secure}
    , user{other.user}
    , password{other.password}
    , db{other.db}
    , table{other.table}
    , where{other.where}
    , update_field{other.update_field}
    , invalidate_query{other.invalidate_query}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, db, table, where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , context(other.context)
    , is_local{other.is_local}
    , pool{is_local ? nullptr : createPool(host, port, secure, db, user, password, context)}
    , load_all_query{other.load_all_query}
{
}

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
    return createStreamForSelectiveLoad(query_builder.composeLoadIdsQuery(ids));
}


BlockInputStreamPtr ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    return createStreamForSelectiveLoad(
        query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES));
}

bool ClickHouseDictionarySource::isModified() const
{
    if (!invalidate_query.empty())
    {
        auto response = doInvalidateQuery(invalidate_query);
        LOG_TRACE(log, "Invalidate query has returned: " << response << ", previous value: " << invalidate_query_response);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
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

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");
    if (is_local)
    {
        Context query_context = context;
        auto input_block = executeQuery(request, query_context, true).in;
        return readInvalidateQuery(*input_block);
    }
    else
    {
        /// We pass empty block to RemoteBlockInputStream, because we don't know the structure of the result.
        Block invalidate_sample_block;
        RemoteBlockInputStream invalidate_stream(pool, request, invalidate_sample_block, context);
        return readInvalidateQuery(invalidate_stream);
    }
}


void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto createTableSource = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 Context & context) -> DictionarySourcePtr
    {
        return std::make_unique<ClickHouseDictionarySource>(dict_struct, config, config_prefix + ".clickhouse", sample_block, context);
    };
    factory.registerSource("clickhouse", createTableSource);
}

}
