#include "ClickHouseDictionarySource.h"
#include <memory>
#include <Client/ConnectionPool.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/ConvertingBlockInputStream.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/executeQuery.h>
#include <Common/isLocalAddress.h>
#include <common/logger_useful.h>
#include "DictionarySourceFactory.h"
#include "DictionaryStructure.h"
#include "ExternalQueryBuilder.h"
#include "readInvalidateQuery.h"
#include "writeParenthesisedString.h"
#include "DictionaryFactory.h"
#include "DictionarySourceHelpers.h"

namespace DB
{

static const size_t MAX_CONNECTIONS = 16;

inline static UInt16 getPortFromContext(const Context & context, bool secure)
{
    return secure ? context.getTCPPortSecure().value_or(0) : context.getTCPPort();
}

static ConnectionPoolWithFailoverPtr createPool(
    const std::string & host,
    UInt16 port,
    bool secure,
    const std::string & db,
    const std::string & user,
    const std::string & password)
{
    ConnectionPoolPtrs pools;
    pools.emplace_back(std::make_shared<ConnectionPool>(
        MAX_CONNECTIONS,
        host,
        port,
        db,
        user,
        password,
        "", /* cluster */
        "", /* cluster_secret */
        "ClickHouseDictionarySource",
        Protocol::Compression::Enable,
        secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));
    return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
}


ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & path_to_settings,
    const std::string & config_prefix,
    const Block & sample_block_,
    const Context & context_,
    const std::string & default_database)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , secure(config.getBool(config_prefix + ".secure", false))
    , host{config.getString(config_prefix + ".host", "localhost")}
    , port(config.getInt(config_prefix + ".port", getPortFromContext(context_, secure)))
    , user{config.getString(config_prefix + ".user", "default")}
    , password{config.getString(config_prefix + ".password", "")}
    , db{config.getString(config_prefix + ".db", default_database)}
    , table{config.getString(config_prefix + ".table")}
    , where{config.getString(config_prefix + ".where", "")}
    , update_field{config.getString(config_prefix + ".update_field", "")}
    , invalidate_query{config.getString(config_prefix + ".invalidate_query", "")}
    , query_builder{dict_struct, db, "", table, where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block_}
    , context(context_)
    , is_local{isLocalAddress({host, port}, getPortFromContext(context_, secure))}
    , pool{is_local ? nullptr : createPool(host, port, secure, db, user, password)}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
    /// We should set user info even for the case when the dictionary is loaded in-process (without TCP communication).
    if (is_local)
    {
        context.setUser(user, password, Poco::Net::SocketAddress("127.0.0.1", 0));
        context = copyContextAndApplySettings(path_to_settings, context, config);
    }

    /// Query context is needed because some code in executeQuery function may assume it exists.
    /// Current example is Context::getSampleBlockCache from InterpreterSelectWithUnionQuery::getSampleBlock.
    context.makeQueryContext();
}


ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , secure{other.secure}
    , host{other.host}
    , port{other.port}
    , user{other.user}
    , password{other.password}
    , db{other.db}
    , table{other.table}
    , where{other.where}
    , update_field{other.update_field}
    , invalidate_query{other.invalidate_query}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, db, "", table, where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , context(other.context)
    , is_local{other.is_local}
    , pool{is_local ? nullptr : createPool(host, port, secure, db, user, password)}
    , load_all_query{other.load_all_query}
{
    context.makeQueryContext();
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
        return query_builder.composeLoadAllQuery();
    }
}

BlockInputStreamPtr ClickHouseDictionarySource::loadAll()
{
    /** Query to local ClickHouse is marked internal in order to avoid
      *    the necessity of holding process_list_element shared pointer.
      */
    if (is_local)
    {
        auto stream = executeQuery(load_all_query, context, true).getInputStream();
        /// FIXME res.in may implicitly use some objects owned be res, but them will be destructed after return
        stream = std::make_shared<ConvertingBlockInputStream>(stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Position);
        return stream;
    }
    return std::make_shared<RemoteBlockInputStream>(pool, load_all_query, sample_block, context);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadUpdatedAll()
{
    std::string load_update_query = getUpdateFieldAndDate();
    if (is_local)
    {
        auto stream = executeQuery(load_update_query, context, true).getInputStream();
        stream = std::make_shared<ConvertingBlockInputStream>(stream, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Position);
        return stream;
    }
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
        LOG_TRACE(log, "Invalidate query has returned: {}, previous value: {}", response, invalidate_query_response);
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
    {
        auto res = executeQuery(query, context, true).getInputStream();
        res = std::make_shared<ConvertingBlockInputStream>(
            res, sample_block, ConvertingBlockInputStream::MatchColumnsMode::Position);
        return res;
    }

    return std::make_shared<RemoteBlockInputStream>(pool, query, sample_block, context);
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");
    if (is_local)
    {
        Context query_context = context;
        auto input_block = executeQuery(request, query_context, true).getInputStream();
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
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 const Context & context,
                                 const std::string & default_database,
                                 bool /* check_config */) -> DictionarySourcePtr
    {
        return std::make_unique<ClickHouseDictionarySource>(
            dict_struct, config, config_prefix, config_prefix + ".clickhouse", sample_block, context, default_database);
    };
    factory.registerSource("clickhouse", create_table_source);
}

}
