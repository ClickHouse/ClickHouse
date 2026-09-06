#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Poco/Net/SocketAddress.h>
#include <memory>
#include <Client/ConnectionPool.h>
#include <Common/CurrentThread.h>
#include <Common/QueryScope.h>
#include <Common/DateLUTImpl.h>
#include <Common/RemoteHostFilter.h>
#include <Common/SettingSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <IO/ConnectionTimeouts.h>
#include <Interpreters/Session.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/Context.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Common/logger_useful.h>
#include <QueryPipeline/BlockIO.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Access/AccessControl.h>
#include <Access/Common/SQLSecurityDefs.h>
#include <Access/User.h>
#include <base/EnumReflection.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExternalQueryBuilder.h>
#include <Dictionaries/readInvalidateQuery.h>
#include <Dictionaries/DictionaryFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr size_t MAX_CONNECTIONS = 16;

    /// A dictionary source's config prefix is always the dictionary root's `.source` node
    /// (`DictionaryFactory::create` builds it that way), so the root is everything before the last
    /// component; `sql_security` / `definer` are recorded on the root next to `comment`.
    std::string getDictionaryRootPrefix(const std::string & source_config_prefix)
    {
        auto dot = source_config_prefix.rfind('.');
        if (dot == std::string::npos)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Expected a dictionary source config prefix to have dotted components, got: {}", source_config_prefix);
        return source_config_prefix.substr(0, dot);
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
            configuration.proto_send_chunked,
            configuration.proto_recv_chunked,
            configuration.quota_key,
            "", /* cluster */
            "", /* cluster_secret */
            "ClickHouseDictionarySource",
            Protocol::Compression::Enable,
            configuration.secure ? Protocol::Secure::Enable : Protocol::Secure::Disable,
            "" /* bind_host */));

        return std::make_shared<ConnectionPoolWithFailover>(pools, LoadBalancing::RANDOM);
    }

}

ClickHouseDictionarySource::ClickHouseDictionarySource(
    const DictionaryStructure & dict_struct_,
    const Configuration & configuration_,
    const Block & sample_block_,
    ContextMutablePtr context_)
    : update_time{std::chrono::system_clock::from_time_t(0)}
    , dict_struct{dict_struct_}
    , configuration{configuration_}
    , query_builder(std::make_shared<ExternalQueryBuilder>(dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks))
    , sample_block{sample_block_}
    , context(context_)
    , pool{createPool(configuration)}
    , load_all_query{query_builder->composeLoadAllQuery()}
{
}

ClickHouseDictionarySource::ClickHouseDictionarySource(const ClickHouseDictionarySource & other)
    : update_time{other.update_time}
    , dict_struct{other.dict_struct}
    , configuration{other.configuration}
    , invalidate_query_response{other.invalidate_query_response}
    , query_builder(std::make_shared<ExternalQueryBuilder>(dict_struct, configuration.db, "", configuration.table, configuration.query, configuration.where, IdentifierQuotingStyle::Backticks))
    , sample_block{other.sample_block}
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
        return query_builder->composeUpdateQuery(configuration.update_field, str_time);
    }

    update_time = std::chrono::system_clock::now();
    return query_builder->composeLoadAllQuery();
}

BlockIO ClickHouseDictionarySource::loadAll()
{
    return createStreamForQuery(load_all_query);
}

BlockIO ClickHouseDictionarySource::loadUpdatedAll()
{
    String load_update_query = getUpdateFieldAndDate();
    return createStreamForQuery(load_update_query);
}

BlockIO ClickHouseDictionarySource::loadIds(const VectorWithMemoryTracking<UInt64> & ids)
{
    return createStreamForQuery(query_builder->composeLoadIdsQuery(ids));
}


BlockIO ClickHouseDictionarySource::loadKeys(const Columns & key_columns, const VectorWithMemoryTracking<size_t> & requested_rows)
{
    String query = query_builder->composeLoadKeysQuery(key_columns, requested_rows, ExternalQueryBuilder::IN_WITH_TUPLES);
    return createStreamForQuery(query);
}

bool ClickHouseDictionarySource::isModified() const
{
    if (!configuration.invalidate_query.empty())
    {
        auto response = doInvalidateQuery(configuration.invalidate_query);
        LOG_TRACE(log, "Invalidate query has returned: {}", response);
        return invalidate_query_response.updateAndCheckModified(response);
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

BlockIO ClickHouseDictionarySource::createStreamForQuery(const String & query)
{
    BlockIO io;

    /// Sample block should not contain first row default values
    auto empty_sample_block = std::make_shared<const Block>(sample_block.cloneEmpty());

    /// Copy context because results of scalar subqueries potentially could be cached
    auto context_copy = Context::createCopy(context);
    context_copy->makeQueryContext();

    const char * query_begin = query.data();
    const char * query_end = query.data() + query.size();
    ParserQuery parser(query_end);
    ASTPtr ast = parseQuery(parser, query_begin, query_end, "Query for ClickHouse dictionary", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    if (!ast || ast->getQueryKind() != IAST::QueryKind::Select)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Only SELECT query can be used as a dictionary source");

    if (configuration.is_local)
    {
        context_copy->setCurrentQueryId({});

        if (!CurrentThread::getGroup())
            io.query_scope = QueryScope::create(context_copy);

        io = executeQuery(query, context_copy, QueryFlags{ .internal = true }).second;

        io.pipeline.convertStructureTo(empty_sample_block->getColumnsWithTypeAndName(), context_copy);
    }
    else
    {
        io.pipeline = QueryPipeline(std::make_shared<RemoteSource>(
            std::make_shared<RemoteQueryExecutor>(pool, query, empty_sample_block, std::move(context_copy)), false, false, false));
    }

    return io;
}

std::string ClickHouseDictionarySource::doInvalidateQuery(const std::string & request) const
{
    LOG_TRACE(log, "Performing invalidate query");

    /// Copy context because results of scalar subqueries potentially could be cached
    auto context_copy = Context::createCopy(context);
    context_copy->makeQueryContext();
    context_copy->setCurrentQueryId("");

    if (configuration.is_local)
    {
        QueryScope query_scope;
        if (!CurrentThread::getGroup())
            query_scope = QueryScope::create(context_copy);

        BlockIO io = executeQuery(request, context_copy, QueryFlags{ .internal = true }).second;
        std::string result;
        io.executeWithCallbacks([&]()
        {
            result = readInvalidateQuery(io.pipeline);
        });
        return result;
    }

    /// We pass empty block to RemoteQueryExecutor, because we don't know the structure of the result.
    auto invalidate_sample_block = std::make_shared<const Block>(Block{});
    QueryPipeline pipeline(std::make_shared<RemoteSource>(
        std::make_shared<RemoteQueryExecutor>(pool, request, invalidate_sample_block, context_copy), false, false, false));
    return readInvalidateQuery(pipeline);
}

void registerDictionarySourceClickHouse(DictionarySourceFactory & factory);
void registerDictionarySourceClickHouse(DictionarySourceFactory & factory)
{
    auto create_table_source = [=](const String & /*name*/,
                                 const DictionaryStructure & dict_struct,
                                 const Poco::Util::AbstractConfiguration & config,
                                 const std::string & config_prefix,
                                 Block & sample_block,
                                 ContextPtr global_context,
                                 const std::string & default_database,
                                 bool created_from_ddl) -> DictionarySourcePtr
    {
        auto configuration = ClickHouseDictionarySource::resolveConfiguration(
            config,
            config_prefix,
            global_context,
            default_database,
            created_from_ddl,
            NamedCollectionUsage::CheckAccessAndRegisterDependency);

        /// A definer is only honoured for the DDL shape that validated it: the CREATE path always
        /// records `sql_security` next to `definer`, so a lone `<definer>` element hand-written in a
        /// config file stays inert rather than silently authenticating the load as that user.
        const auto root_prefix = getDictionaryRootPrefix(config_prefix);
        const auto definer = created_from_ddl
                && config.getString(root_prefix + ".sql_security", "") == magic_enum::enum_name(SQLSecurityType::DEFINER)
            ? config.getString(root_prefix + ".definer", "")
            : "";

        ContextMutablePtr context;
        if (!definer.empty())
        {
            /// Only a local source can honour a definer: a remote one authenticates over the wire
            /// against another server, which cannot be asked to trust an identity resolved here.
            if (!configuration.is_local)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "A definer is only supported for a dictionary whose CLICKHOUSE source is local, "
                    "but the source of this dictionary points at {}:{}", configuration.host, configuration.port);

            context = Context::createCopy(global_context);
            context->makeQueryContext();
            context->setUser(global_context->getAccessControl().getID<User>(definer));
            context->setCurrentUserName(definer);
            context->setInitialUserName(definer);
            /// The load runs in-process, so the peer is this server, not the wildcard address a
            /// global-context copy carries.
            context->setCurrentAddress(Poco::Net::SocketAddress{"127.0.0.1", 0});
            context->setInitialAddress(Poco::Net::SocketAddress{"127.0.0.1", 0});
        }
        else if (configuration.is_local)
        {
            /// We should set user info even for the case when the dictionary is loaded in-process (without TCP communication).
            Session session(global_context, ClientInfo::Interface::LOCAL);
            session.authenticate(configuration.user, configuration.password, Poco::Net::SocketAddress{});
            context = session.makeQueryContext();
        }
        else
        {
            context = Context::createCopy(global_context);

            if (created_from_ddl)
                context->getRemoteHostFilter().checkHostAndPort(configuration.host, toString(configuration.port));
        }

        auto settings_changes = readSettingsFromDictionaryConfig(config, config_prefix);
        /// The definer's own constraints bound what the dictionary definition can set, the same way
        /// `StorageInMemoryMetadata::getSQLSecurityOverriddenContext` bounds a view's triggering query.
        if (!definer.empty())
            context->clampToSettingsConstraints(settings_changes, SettingSource::QUERY);
        context->applySettingsChanges(settings_changes);

        String dictionary_name = config.getString(".dictionary.name", "");
        String dictionary_database = config.getString(".dictionary.database", "");

        if (dictionary_name == configuration.table && dictionary_database == configuration.db)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "ClickHouseDictionarySource table cannot be dictionary table");

        return std::make_unique<ClickHouseDictionarySource>(dict_struct, configuration, sample_block, context);
    };

    factory.registerSource("clickhouse", create_table_source, Documentation{
        .description = R"DOCS_MD(
# ClickHouse dictionary source

Example of settings:

<Tabs>
<Tab title="DDL">

```sql
SOURCE(CLICKHOUSE(
    host 'example01-01-1'
    port 9000
    user 'default'
    password ''
    db 'default'
    table 'ids'
    where 'id=10'
    secure 1
    query 'SELECT id, value_1, value_2 FROM default.ids'
));
```

</Tab>
<Tab title="Configuration file">

```xml
<source>
    <clickhouse>
        <host>example01-01-1</host>
        <port>9000</port>
        <user>default</user>
        <password></password>
        <db>default</db>
        <table>ids</table>
        <where>id=10</where>
        <secure>1</secure>
        <query>SELECT id, value_1, value_2 FROM default.ids</query>
    </clickhouse>
</source>
```

</Tab>
</Tabs>
<br/>

Setting fields:

| Setting | Description |
|---------|-------------|
| `host` | The ClickHouse host. If it is a local host, the query is processed without any network activity. To improve fault tolerance, you can create a [Distributed](/reference/engines/table-engines/special/distributed) table and enter it in subsequent configurations. |
| `port` | The port on the ClickHouse server. |
| `user` | Name of the ClickHouse user. |
| `password` | Password of the ClickHouse user. |
| `db` | Name of the database. |
| `table` | Name of the table. |
| `where` | The selection criteria. Optional. |
| `invalidate_query` | Query for checking the dictionary status. Optional. Read more in the section [Refreshing dictionary data using LIFETIME](/reference/statements/create/dictionary/lifetime). |
| `secure` | Use SSL for connection. |
| `query` | The custom query. Optional. |

<Note>
The `table` or `where` fields cannot be used together with the `query` field. And either one of the `table` or `query` fields must be declared.
</Note>
)DOCS_MD",
        .syntax = "SOURCE(CLICKHOUSE(host 'host' port 9000 user 'default' password '' db 'db' table 'table'))",
        .related = {"mysql", "postgresql"}});
}

}
