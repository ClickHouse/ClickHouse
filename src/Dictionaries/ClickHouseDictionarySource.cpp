#include "ClickHouseDictionarySource.h"
#include <memory>
#include <Client/ConnectionPool.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Common/isLocalAddress.h>
#include <base/logger_useful.h>
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
    ContextMutablePtr context_,
    std::shared_ptr<Session> local_session_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{sample_block_}
    , local_session(local_session_)
    , context(context_)
    , pool{createPool(configuration)}
    , load_all_query{query_builder.composeLoadAllQuery()}
{
}

ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder{dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks}
    , sample_block{other.sample_block}
    , local_session(other.local_session)
    , context(Context::createCopy(other.context))
    , pool{createPool(configuration)}
    , load_all_query{other.load_all_query}
{
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

Pipe ClickHouseDictionarySource::loadAllWithSizeHint(std::atomic<size_t> * result_size_hint)
{
    return createStreamForQuery(load_all_query, result_size_hint);
}

Pipe ClickHouseDictionarySource::loadAll()
{
    return createStreamForQuery(load_all_query);
}

Pipe ClickHouseDictionarySource::loadUpdatedAll()
{
    String load_update_query = getUpdateFieldAndDate();
    return createStreamForQuery(load_update_query);
}

Pipe ClickHouseDictionarySource::loadIds(const std::vector<UInt64> & ids)
{
    return createStreamForQuery(query_builder.composeLoadIdsQuery(ids));
}


Pipe ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
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

Pipe ClickHouseDictionarySource::createStreamForQuery(const String & query, std::atomic<size_t> * result_size_hint)
{
    QueryPipelineBuilder builder;

    /// Sample block should not contain first row default values
    auto empty_sample_block = sample_block.cloneEmpty();

    if (configuration.is_local)
    {
        builder.init(executeQuery(query, context, true).pipeline);
        auto converting = ActionsDAG::makeConvertingActions(
            builder.getHeader().getColumnsWithTypeAndName(),
            empty_sample_block.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position);

        builder.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, std::make_shared<ExpressionActions>(converting));
        });
    }
    else
    {
        builder.init(Pipe(std::make_shared<RemoteSource>(
            std::make_shared<RemoteQueryExecutor>(pool, query, empty_sample_block, context), false, false)));
    }

    if (result_size_hint)
    {
        builder.setProgressCallback([result_size_hint](const Progress & progress)
        {
            *result_size_hint += progress.total_rows_to_read;
        });
    }

    return QueryPipelineBuilder::getPipe(std::move(builder));
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");
    if (configuration.is_local)
    {
        auto query_context = Context::createCopy(context);
        return readInvalidateQuery(executeQuery(request, query_context, true).pipeline);
    }
    else
    {
        /// We pass empty block to RemoteBlockInputStream, because we don't know the structure of the result.
        Block invalidate_sample_block;
        QueryPipeline pipeline(std::make_shared<RemoteSource>(
            std::make_shared<RemoteQueryExecutor>(pool, request, invalidate_sample_block, context), false, false));
        return readInvalidateQuery(std::move(pipeline));
    }
}

void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & default_database [[maybe_unused]],
                                 bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        bool secure = config.getBool(config_prefix + ".secure", false);

        UInt16 default_port = getPortFromContext(global_context, secure);

        std::string settings_config_prefix = config_prefix + ".clickhouse";
        std::string host = config.getString(settings_config_prefix + ".host", "localhost");
        UInt16 port = static_cast<UInt16>(config.getUInt(settings_config_prefix + ".port", default_port));

        ClickHouseDictionarySource::Configuration configuration
        {
            .host = host,
            .user = config.getString(settings_config_prefix + ".user", "default"),
            .password = config.getString(settings_config_prefix + ".password", ""),
            .db = config.getString(settings_config_prefix + ".db", default_database),
            .table = config.getString(settings_config_prefix + ".table", ""),
            .query = config.getString(settings_config_prefix + ".query", ""),
            .where = config.getString(settings_config_prefix + ".where", ""),
            .invalidate_query = config.getString(settings_config_prefix + ".invalidate_query", ""),
            .update_field = config.getString(settings_config_prefix + ".update_field", ""),
            .update_lag = config.getUInt64(settings_config_prefix + ".update_lag", 1),
            .port = port,
            .is_local = isLocalAddress({host, port}, default_port),
            .secure = config.getBool(settings_config_prefix + ".secure", false)
        };

        ContextMutablePtr context;
        std::shared_ptr<Session> local_session;
        if (configuration.is_local)
        {
            /// Start local session in case when the dictionary is loaded in-process (without TCP communication).
            local_session = std::make_shared<Session>(global_context, ClientInfo::Interface::LOCAL);
            local_session->authenticate(configuration.user, configuration.password, {});
            context = local_session->makeQueryContext();
            context->applySettingsChanges(readSettingsFromDictionaryConfig(config, config_prefix));
        }
        else
            context = copyContextAndApplySettingsFromDictionaryConfig(global_context, config, config_prefix);

        String dictionary_name = config.getString(".dictionary.name", "");
        String dictionary_database = config.getString(".dictionary.database", "");

        if (dictionary_name == configuration.table && dictionary_database == configuration.db)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouseDictionarySource table cannot be dictionary table");

        return std::make_unique<ClickHouseDictionarySource>(dict_struct, configuration, sample_block, context, local_session);
    };

    factory.registerSource("clickhouse", create_table_source);
}

}
