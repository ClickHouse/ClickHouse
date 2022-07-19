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

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{
    constexpr size_t MAX_CONNECTIONS = 16;

    inline UInt16 getPortFromContext(ContextPtr context, bool secure)
    {
        return secure ? context->getTCPPortSecure().value_or(0) : context->getTCPPort();
    }

    ConnectionPoolWithFailoverPtr createPool(const ClickHouseDictionarySource::Configuration & configuration)
    {
        if (configuration.is_local)
            return nullptr;

        ConnectionPoolPtrs pools;
        pools.emplace_back(std::make_shared<ConnectionPool>(
            MAX_CONNECTIONS,
            configuration.host,
            configuration.port,
            configuration.db,
            configuration.user,
            configuration.password,
            "", /* cluster */
            "", /* cluster_secret */
            "ClickHouseDictionarySource",
            Protocol::Compression::Enable,
            configuration.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable));

        return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
    }

}

ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    const Block & sample_block_,
    ContextPtr context_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block_}
    , context(Context::createCopy(context_))
    , pool{createPool(configuration)}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
    /// Query context is needed because some code in executeQuery function may assume it exists.
    /// Current example is Context::getSampleBlockCache from InterpreterSelectWithUnionQuery::getSampleBlock.
    context->makeQueryContext();
}

ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , context(Context::createCopy(other.context))
    , pool{createPool(configuration)}
    , load_all_query{other.load_all_query}
{
    context->makeQueryContext();
}

std::string ClickHouseDictionarySource::getUpdateFieldAndDate()
{
    if (update_time != std::chrono::system_clock::from_time_t(0))
    {
        time_t hr_time = std::chrono::system_clock::to_time_t(update_time) - configuration.update_lag;
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

BlockInputStreamPtr ClickHouseDictionarySource::loadAllWithSizeHint(std::atomic<size_t> * result_size_hint)
{
    return createStreamForQuery(load_all_query, result_size_hint);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadAll()
{
    return createStreamForQuery(load_all_query);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadUpdatedAll()
{
    String load_update_query = getUpdateFieldAndDate();
    return createStreamForQuery(load_update_query);
}

BlockInputStreamPtr ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    return createStreamForQuery(query_builder.composeLoadIdsQuery(ids));
}


BlockInputStreamPtr ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    String query = query_builder.composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES);
    return createStreamForQuery(query);
}

bool ClickHouseDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        LOG_TRACE(log, "Invalidate query has returned: {}, previous value: {}", response, invalidate_query_response);
        if (invalidate_query_response == response)
            return false;
        invalidate_query_response = response;
    }
    return true;
}

bool ClickHouseDictionarySource::hasUpdateField() const
{
    return !configuration.update_field.empty();
}

std::string ClickHouseDictionarySource::toString() const
{
    const std::string & where = configuration.where;
    return "ClickHouse: " + configuration.db + '.' + configuration.table + (where.empty() ? "" : ", where: " + where);
}

BlockInputStreamPtr ClickHouseDictionarySource::createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint)
{
    BlockInputStreamPtr stream;

    /// Sample block should not contain first row default values
    auto empty_sample_block = sample_block.cloneEmpty();

    if (configuration.is_local)
    {
        stream = executeQuery(query, context, true).getInputStream();
        stream = std::make_shared<ConvertingBlockInputStream>(stream, empty_sample_block, ConvertingBlockInputStream::MatchColumnsMode::Position);
    }
    else
    {
        stream = std::make_shared<RemoteBlockInputStream>(pool, query, empty_sample_block, context);
    }

    if (result_size_hint)
    {
        stream->setProgressCallback([result_size_hint](const Progress & progress)
        {
            *result_size_hint += progress.total_rows_to_read;
        });
    }

    return stream;
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");
    if (configuration.is_local)
    {
        auto query_context = Context::createCopy(context);
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
                                 ContextPtr context,
                                 const std::string & default_database [[maybe_unused]],
                                 bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        bool secure = config.getBool(config_prefix + ".secure", false);
        auto context_copy = Context::createCopy(context);

        UInt16 default_port = getPortFromContext(context_copy, secure);

        std::string settings_config_prefix = config_prefix + ".clickhouse";
        std::string host = config.getString(settings_config_prefix + ".host", "localhost");
        UInt16 port = static_cast<UInt16>(config.getUInt(settings_config_prefix + ".port", default_port));

        ClickHouseDictionarySource::Configuration configuration
        {
            .host = host,
            .user = config.getString(settings_config_prefix + ".user", "default"),
            .password = config.getString(settings_config_prefix + ".password", ""),
            .db = config.getString(settings_config_prefix + ".db", default_database),
            .table = config.getString(settings_config_prefix + ".table"),
            .where = config.getString(settings_config_prefix + ".where", ""),
            .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .port = port,
            .is_local = isLocalAddress({host, port}, default_port),
            .secure = config.getBool(settings_config_prefix + ".secure", false)
        };

        /// We should set user info even for the case when the dictionary is loaded in-process (without TCP communication).
        if (configuration.is_local)
        {
            context_copy->setUser(configuration.user, configuration.password, Poco::Net::SocketAddress("127.0.0.1", 0));
            context_copy = copyContextAndApplySettings(config_prefix, context_copy, config);
        }

        String dictionary_name = config.getString(".dictionary.name", "");
        String dictionary_database = config.getString(".dictionary.database", "");

        if (dictionary_name == configuration.table && dictionary_database == configuration.db)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouseDictionarySource table cannot be dictionary table");

        return std::make_unique<ClickHouseDictionarySource>(dict_struct, configuration, sample_block, context_copy);
    };

    factory.registerSource("clickhouse", create_table_source);
}

}
