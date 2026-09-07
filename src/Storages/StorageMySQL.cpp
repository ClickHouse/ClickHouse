#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageMySQL.h>

#if USE_MYSQL

#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/convertMySQLDataType.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <mysqlxx/Transaction.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/Pipe.h>
#include <Columns/IColumn.h>
#include <Common/RemoteHostFilter.h>
#include <Common/parseRemoteDescription.h>
#include <Common/quoteString.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Databases/MySQL/FetchTablesColumnsList.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool external_table_functions_use_nulls;
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
    extern const SettingsUInt64 mysql_max_rows_to_insert;
}

namespace MySQLSetting
{
    extern const MySQLSettingsBool connection_auto_close;
    extern const MySQLSettingsUInt64 connection_pool_size;
    extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
}

namespace
{
/// Infer the structure of a result of a user-provided query by executing it with a `LIMIT 0` on the MySQL side.
ColumnsDescription doQueryResultStructure(
    mysqlxx::PoolWithFailover & pool_, const String & select_query, const ContextPtr & context_,
    MultiEnum<MySQLDataTypesSupport> type_support);
}

StorageMySQL::StorageMySQL(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const TableNameOrQuery & remote_table_or_query_,
    const bool replace_query_,
    const std::string & on_duplicate_clause_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_,
    const MySQLSettings & mysql_settings_)
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_database_name(remote_database_name_)
    , remote_table_or_query(remote_table_or_query_)
    , replace_query{replace_query_}
    , on_duplicate_clause{on_duplicate_clause_}
    , mysql_settings(std::make_unique<MySQLSettings>(mysql_settings_))
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , log(getLogger("StorageMySQL (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        /// Schema inference must honor the type-mapping configuration of this engine instance
        /// (its own SETTINGS or the value bridged in from the query context at creation time),
        /// not the global query-context setting, otherwise an explicit per-engine opt-out such as
        /// `SETTINGS mysql_datatypes_support_level = '...'` would be silently ignored.
        auto columns = getTableStructureFromData(
            *pool, remote_database_name, remote_table_or_query, context_,
            (*mysql_settings)[MySQLSetting::mysql_datatypes_support_level]);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageMySQL::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

ColumnsDescription StorageMySQL::getTableStructureFromData(
    mysqlxx::PoolWithFailover & pool_,
    const String & database,
    const TableNameOrQuery & table_or_query,
    const ContextPtr & context_,
    MultiEnum<MySQLDataTypesSupport> type_support)
{
    if (table_or_query.isQuery())
        return doQueryResultStructure(pool_, table_or_query.getQuery(), context_, type_support);

    const auto & table = table_or_query.getTableName();
    const auto & settings = context_->getSettingsRef();
    const auto tables_and_columns = fetchTablesColumnsList(pool_, database, {table}, settings, type_support);

    const auto columns = tables_and_columns.find(table);
    if (columns == tables_and_columns.end())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "MySQL table {} doesn't exist.",
                        (database.empty() ? "" : (backQuote(database) + "." + backQuote(table))));

    return columns->second;
}

void StorageMySQL::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t /*max_block_size*/,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    String query;
    if (remote_table_or_query.isQuery())
    {
        /// The user-provided query is passed to MySQL as is; no outer predicate is pushed down into it, so
        /// reject any outer filter under external_table_strict_query.
        rejectOuterFilterForQueryBackedExternalSourceIfStrict(query_info, context_);
        query = buildQueryForExternalDatabaseSubquery(remote_table_or_query.getQuery(), column_names, IdentifierQuotingStyle::BackticksMySQL);
    }
    else
        query = transformQueryForExternalDatabase(
            query_info,
            column_names,
            storage_snapshot->metadata->getColumns().getOrdinary(),
            IdentifierQuotingStyle::BackticksMySQL,
            LiteralEscapingStyle::Regular,
            remote_database_name,
            remote_table_or_query.getTableName(),
            context_);
    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);

        WhichDataType which(column_data.type);
        /// Convert enum to string.
        if (which.isEnum())
            column_data.type = std::make_shared<DataTypeString>();
        sample_block.insert({ column_data.type, column_data.name });
    }

    MySQLStreamSettings mysql_input_stream_settings(context_->getSettingsRef(),
            (*mysql_settings)[MySQLSetting::connection_auto_close]);
    query_plan.addStep(std::make_unique<ReadFromMySQLStep>(
        sample_block,
        pool,
        query,
        mysql_input_stream_settings));
}


class StorageMySQLSink final : public SinkToStorage
{
public:
    explicit StorageMySQLSink(
        const StorageMySQL & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::string & remote_database_name_,
        const std::string & remote_table_name_,
        const mysqlxx::PoolWithFailover::Entry & entry_,
        const size_t & mysql_max_rows_to_insert)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage{storage_}
        , metadata_snapshot{metadata_snapshot_}
        , remote_database_name{remote_database_name_}
        , remote_table_name{remote_table_name_}
        , entry{entry_}
        , max_batch_rows{mysql_max_rows_to_insert}
    {
    }

    String getName() const override { return "StorageMySQLSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        auto blocks = splitBlocks(block, max_batch_rows);
        mysqlxx::Transaction trans(entry);
        try
        {
            for (const Block & batch_data : blocks)
            {
                writeBlockData(batch_data);
            }
            trans.commit();
        }
        catch (...)
        {
            trans.rollback();
            throw;
        }
    }

    void writeBlockData(const Block & block)
    {
        WriteBufferFromOwnString sqlbuf;
        sqlbuf << (storage.replace_query ? "REPLACE" : "INSERT") << " INTO ";
        if (!remote_database_name.empty())
            sqlbuf << backQuoteMySQL(remote_database_name) << ".";
        sqlbuf << backQuoteMySQL(remote_table_name);
        sqlbuf << " (" << dumpNamesWithBackQuote(block) << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputFormat("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.getContext());
        writer->write(block);

        if (!storage.on_duplicate_clause.empty())
            sqlbuf << " ON DUPLICATE KEY " << storage.on_duplicate_clause;

        sqlbuf << ";";

        auto query = this->entry->query(sqlbuf.str());
        query.execute();
    }

    Blocks splitBlocks(const Block & block, const size_t & max_rows) const
    {
        /// Avoid Excessive copy when block is small enough
        if (block.rows() <= max_rows)
            return {block};

        const size_t split_block_size = static_cast<size_t>(ceil(static_cast<double>(block.rows()) * 1.0 / static_cast<double>(max_rows)));
        Blocks split_blocks(split_block_size);

        for (size_t idx = 0; idx < split_block_size; ++idx)
            split_blocks[idx] = block.cloneEmpty();

        const size_t columns = block.columns();
        const size_t rows = block.rows();
        size_t offsets = 0;
        UInt64 limits = max_batch_rows;
        for (size_t idx = 0; idx < split_block_size; ++idx)
        {
            /// For last batch, limits should be the remain size
            if (idx == split_block_size - 1) limits = rows - offsets;
            for (size_t col_idx = 0; col_idx < columns; ++col_idx)
            {
                split_blocks[idx].getByPosition(col_idx).column = block.getByPosition(col_idx).column->cut(offsets, limits);
            }
            offsets += max_batch_rows;
        }

        return split_blocks;
    }

    static std::string dumpNamesWithBackQuote(const Block & block)
    {
        WriteBufferFromOwnString out;
        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                out << ", ";
            out << backQuoteMySQL(it->name);
        }
        return out.str();
    }

private:
    const StorageMySQL & storage;
    StorageMetadataPtr metadata_snapshot;
    std::string remote_database_name;
    std::string remote_table_name;
    mysqlxx::PoolWithFailover::Entry entry;
    size_t max_batch_rows;
};


SinkToStoragePtr StorageMySQL::write(const ASTPtr & /*query*/, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool /*async_insert*/)
{
    if (remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot write into a MySQL table representing the result of a query");

    return std::make_shared<StorageMySQLSink>(
        *this,
        metadata_snapshot,
        remote_database_name,
        remote_table_or_query.getTableName(),
        pool->get(),
        local_context->getSettingsRef()[Setting::mysql_max_rows_to_insert]);
}

mysqlxx::SSLParams StorageMySQL::getSSLParams(const NamedCollection & named_collection)
{
    /// A path to a certificate or a key is only accepted from the server configuration file: the
    /// server opens the file with its own privileges, so taking a path from SQL would let anyone who
    /// can define a MySQL source probe the local filesystem, and authenticate with a client
    /// certificate they are not allowed to read themselves.
    const bool from_config = named_collection.getSourceId() == NamedCollection::SourceId::CONFIG;

    auto get_path = [&](const std::string & key, const std::string & contents_key)
    {
        auto value = named_collection.getOrDefault<String>(key, "");

        /// Checked before the empty fast path: overriding a configured path with the empty string
        /// would silently drop the credential the operator configured, e.g. disable the verification
        /// of the server certificate against `ssl_ca`.
        if (named_collection.isQueryOverridden(key))
        {
            /// Outside the server configuration file the path key is not accepted at all, overridden
            /// or not, so the override rejection would name the wrong remedy.
            if (!from_config)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "`{}` can only be specified in a named collection defined in the server configuration file. "
                    "Pass the contents of the file in `{}` instead",
                    key, contents_key);

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` cannot be overridden in a query. "
                "Pass the contents of the file in `{}` instead",
                key, contents_key);
        }

        /// An empty contents override never replaces the stored credential with another one - it can
        /// only silently drop whatever form of it the collection carries, a path or the contents
        /// alike. Checked before the empty fast path below so a credential the collection stores in
        /// the contents form is protected too. When the collection stores no credential at all
        /// (neither the path - a query cannot override it, so `value` is the collection's own - nor
        /// the contents, read in its pre-override form), there is nothing to drop and the empty
        /// override stays the no-op it is for the direct arguments.
        if (named_collection.isQueryOverridden(contents_key) && named_collection.getOrDefault<String>(contents_key, "").empty()
            && (!value.empty() || !named_collection.getValueBeforeQueryOverride(contents_key).value_or("").empty()))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` cannot be overridden with an empty `{}`", key, contents_key);

        if (value.empty())
            return value;

        if (!from_config)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "`{}` can only be specified in a named collection defined in the server configuration file. "
                "Pass the contents of the file in `{}` instead",
                key, contents_key);

        /// The contents are the SQL-safe form of the same credential, so a query passing them replaces
        /// the path inherited from the collection - that is the only way to override the credential
        /// from SQL at all. Both forms coming from the collection definition itself remain ambiguous.
        if (named_collection.isQueryOverridden(contents_key))
        {
            /// A credential the operator explicitly locked (`<ssl_ca overridable="false">`) cannot be
            /// replaced through the contents form either.
            if (!named_collection.isOverridable(key, /* default_value= */ true))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Override not allowed for '{}'", key);

            return String{};
        }

        if (!named_collection.getOrDefault<String>(contents_key, "").empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "`{}` and `{}` cannot be specified at the same time", key, contents_key);

        return value;
    };

    mysqlxx::SSLParams ssl_params;
    ssl_params.ca_path = get_path("ssl_ca", "ssl_ca_pem");
    ssl_params.cert_path = get_path("ssl_cert", "ssl_cert_pem");
    ssl_params.key_path = get_path("ssl_key", "ssl_key_pem");
    ssl_params.ca_pem = named_collection.getOrDefault<String>("ssl_ca_pem", "");
    ssl_params.cert_pem = named_collection.getOrDefault<String>("ssl_cert_pem", "");
    ssl_params.key_pem = named_collection.getOrDefault<String>("ssl_key_pem", "");
    return ssl_params;
}

mysqlxx::SSLParams StorageMySQL::extractSSLParamsFromArguments(ASTs & arguments, ContextPtr context_)
{
    /// The TLS credentials may follow the positional arguments as `key = value` pairs. Only the
    /// contents are accepted there: a path is taken from the server configuration file alone, for the
    /// reason described at `getSSLParams`.
    static const std::initializer_list<std::pair<std::string_view, std::string_view>> tls_keys
        = {{"ssl_ca", "ssl_ca_pem"}, {"ssl_cert", "ssl_cert_pem"}, {"ssl_key", "ssl_key_pem"}};

    std::map<String, String> values;

    size_t num_positional_arguments = arguments.size();
    while (num_positional_arguments > 0)
    {
        const auto * function = arguments[num_positional_arguments - 1]->as<ASTFunction>();
        if (!function || function->name != "equals" || !function->arguments || function->arguments->children.size() != 2)
            break;

        const auto * identifier = function->arguments->children[0]->as<ASTIdentifier>();
        if (!identifier)
            break;

        const String key = identifier->name();
        bool is_contents_key = false;
        for (const auto & [path_key, contents_key] : tls_keys)
        {
            if (key == path_key)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "`{}` can only be specified in a named collection defined in the server configuration file. "
                    "Pass the contents of the file in `{}` instead",
                    path_key, contents_key);
            if (key == contents_key)
                is_contents_key = true;
        }

        /// Anything else is left in place, so that it is reported as an unexpected argument.
        if (!is_contents_key)
            break;

        auto value = evaluateConstantExpressionOrIdentifierAsLiteral(function->arguments->children[1], context_);
        if (!values.emplace(key, checkAndGetLiteralArgument<String>(value, key)).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument `{}` is specified more than once", key);

        --num_positional_arguments;
    }

    arguments.resize(num_positional_arguments);

    auto get = [&](const String & key)
    {
        auto it = values.find(key);
        return it == values.end() ? String{} : it->second;
    };

    mysqlxx::SSLParams ssl_params;
    ssl_params.ca_pem = get("ssl_ca_pem");
    ssl_params.cert_pem = get("ssl_cert_pem");
    ssl_params.key_pem = get("ssl_key_pem");
    return ssl_params;
}

StorageMySQL::Configuration StorageMySQL::processNamedCollectionResult(
    const NamedCollection & named_collection, MySQLSettings & storage_settings, ContextPtr context_, bool require_table_or_query)
{
    StorageMySQL::Configuration configuration;

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments
        = {"replace_query", "on_duplicate_clause", "addresses_expr", "host", "hostname", "port",
           "ssl_ca", "ssl_cert", "ssl_key", "ssl_ca_pem", "ssl_cert_pem", "ssl_key_pem"};
    auto mysql_settings_names = storage_settings.getAllRegisteredNames();
    for (const auto & name : mysql_settings_names)
        optional_arguments.insert(name);

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db"};
    if (require_table_or_query)
    {
        /// The data source is either a remote table or a query passed to MySQL as is.
        if (named_collection.has("query"))
            required_arguments.insert("query");
        else
            required_arguments.insert("table");
    }
    validateNamedCollection<ValidateKeysMultiset<ExternalDatabaseEqualKeysSet>>(named_collection, required_arguments, optional_arguments);

    configuration.addresses_expr = named_collection.getOrDefault<String>("addresses_expr", "");
    if (configuration.addresses_expr.empty())
    {
        configuration.host = named_collection.getAnyOrDefault<String>({"host", "hostname"}, "");
        configuration.port = static_cast<UInt16>(named_collection.get<UInt64>("port"));
        configuration.addresses = {std::make_pair(configuration.host, configuration.port)};
    }
    else
    {
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];
        configuration.addresses = parseRemoteDescriptionForExternalDatabase(
            configuration.addresses_expr, max_addresses, 3306);
    }

    configuration.username = named_collection.getAny<String>({"username", "user"});
    configuration.password = named_collection.get<String>("password");
    configuration.database = named_collection.getAny<String>({"db", "database"});
    if (require_table_or_query)
    {
        if (named_collection.has("query"))
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, named_collection.get<String>("query"));
        else
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, named_collection.get<String>("table"));
    }
    configuration.replace_query = named_collection.getOrDefault<UInt64>("replace_query", false);
    configuration.on_duplicate_clause = named_collection.getOrDefault<String>("on_duplicate_clause", "");
    configuration.ssl_params = getSSLParams(named_collection);

    storage_settings.loadFromNamedCollection(named_collection);

    return configuration;
}

StorageMySQL::Configuration StorageMySQL::getConfiguration(ASTs engine_args, ContextPtr context_, MySQLSettings & storage_settings, const StorageID * table_id)
{
    StorageMySQL::Configuration configuration;
    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context_, true, nullptr, table_id))
    {
        configuration = StorageMySQL::processNamedCollectionResult(*named_collection, storage_settings, context_);
    }
    else
    {
        configuration.ssl_params = extractSSLParamsFromArguments(engine_args, context_);

        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage MySQL requires 5-7 parameters: "
                            "MySQL('host:port' (or 'addresses_pattern'), database, table (or query), "
                            "'user', 'password'[, replace_query, 'on_duplicate_clause']"
                            "[, ssl_ca_pem = '...', ssl_cert_pem = '...', ssl_key_pem = '...']).");

        /// The 3rd argument is either a table name, or a query passed to MySQL as is - `(SELECT ...)` or `query('SELECT ...')`.
        auto maybe_query = tryGetExternalDatabaseQuery(
            engine_args[2], context_, IdentifierQuotingStyle::BackticksMySQL, LiteralEscapingStyle::Regular);
        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            if (i == 2 && maybe_query)
                continue;
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], context_);
        }

        configuration.addresses_expr = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];

        configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        if (maybe_query)
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query);
        else
            configuration.table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(engine_args[2], "table"));
        configuration.username = checkAndGetLiteralArgument<String>(engine_args[3], "username");
        configuration.password = checkAndGetLiteralArgument<String>(engine_args[4], "password");
        if (engine_args.size() >= 6)
            configuration.replace_query = checkAndGetLiteralArgument<UInt64>(engine_args[5], "replace_query");
        if (engine_args.size() == 7)
            configuration.on_duplicate_clause = checkAndGetLiteralArgument<String>(engine_args[6], "on_duplicate_clause");
    }
    for (const auto & address : configuration.addresses)
        context_->getRemoteHostFilter().checkHostAndPort(address.first, toString(address.second));
    if (configuration.replace_query && !configuration.on_duplicate_clause.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Only one of 'replace_query' and 'on_duplicate_clause' can be specified, or none of them");

    return configuration;
}

ReadFromMySQLStep::ReadFromMySQLStep(
    const Block & sample_block_,
    mysqlxx::PoolWithFailoverPtr pool_,
    const std::string & query_str_,
    const MySQLStreamSettings & mysql_input_stream_settings_
)
    : ISourceStep(std::make_shared<const Block>(sample_block_.cloneEmpty()))
    , pool(std::move(pool_))
    , query_str(query_str_)
    , mysql_input_stream_settings(mysql_input_stream_settings_)
{
}

void ReadFromMySQLStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & /*settings*/)
{
    auto pipe = Pipe(std::make_shared<MySQLWithFailoverSource>(pool, query_str, *getOutputHeader(), mysql_input_stream_settings));

    pipeline.init(std::move(pipe));
}


void registerStorageMySQL(StorageFactory & factory);
void registerStorageMySQL(StorageFactory & factory)
{
    factory.registerStorage("MySQL", [](const StorageFactory::Arguments & args)
    {
        MySQLSettings mysql_settings; /// TODO: move some arguments from the arguments to the SETTINGS.
        auto configuration = StorageMySQL::getConfiguration(args.engine_args, args.getLocalContext(), mysql_settings, &args.table_id);

        /// Bridge the query-context value of `mysql_datatypes_support_level` into the engine settings
        /// (and freeze it into the table definition) so that it is honored during schema inference,
        /// the same way the MySQL database engine does. An explicit per-engine SETTINGS value, loaded
        /// right after, takes precedence over it.
        mysql_settings.loadFromQueryContext(args.getLocalContext(), *args.storage_def);
        if (args.storage_def->settings)
            mysql_settings.loadFromQuery(*args.storage_def);

        if (!mysql_settings[MySQLSetting::connection_pool_size])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "connection_pool_size cannot be zero.");

        mysqlxx::PoolWithFailover pool = createMySQLPoolWithFailover(configuration, mysql_settings);

        return std::make_shared<StorageMySQL>(
            args.table_id,
            std::move(pool),
            configuration.database,
            configuration.table_or_query,
            configuration.replace_query,
            configuration.on_duplicate_clause,
            args.columns,
            args.constraints,
            args.comment,
            args.getContext(),
            mysql_settings);
    },
    {
        .supports_settings = true,
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::MYSQL,
        .has_builtin_setting_fn = MySQLSettings::hasBuiltin,
    },
    Documentation{
        .description = R"DOCS_MD(
The MySQL engine allows you to perform `SELECT` and `INSERT` queries on data that is stored on a remote MySQL server.

## Creating a table {#creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300, ]
    [ enable_compression=false ]
;
```

See a detailed description of the [CREATE TABLE](/reference/statements/create/table) query.

The table structure can differ from the original MySQL table structure:

- Column names should be the same as in the original MySQL table, but you can use just some of these columns and in any order.
- Column types may differ from those in the original MySQL table. ClickHouse tries to [cast](/reference/engines/database-engines/mysql#data_types-support) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](/reference/settings/session-settings/external-table#external_table_functions_use_nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

- `host:port` — MySQL server address.
- `database` — Remote database name.
- `table` — Remote table name, or a query passed to MySQL as is (see [Passing a query instead of a table name](#passing-a-query)).
- `user` — MySQL user.
- `password` — User password.
- `replace_query` — Flag that converts `INSERT INTO` queries to `REPLACE INTO`. If `replace_query=1`, the query is substituted.
- `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression that is added to the `INSERT` query.
    Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.
    To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Arguments also can be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `host` and `port` should be specified separately. This approach is recommended for production environment.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

## TLS/SSL {#tls-ssl}

The credentials of an encrypted connection to MySQL are passed as named collection keys (or as key-value arguments):

| Parameter | Description |
|-----------|-------------|
| `ssl_ca_pem` | Contents of the CA certificate that the MySQL server certificate is verified against. |
| `ssl_cert_pem` | Contents of the client certificate, for certificate-based authentication. |
| `ssl_key_pem` | Contents of the private key belonging to `ssl_cert_pem`. |

The values are the contents of the corresponding PEM files, which can be copied into a named collection or into a query. They are masked in logs and in `SHOW` queries, the same way passwords are.

The same credentials can also be given as paths to files on the server, in `ssl_ca`, `ssl_cert` and `ssl_key` — but **only in a named collection defined in the server configuration file**, and such a value cannot be overridden in a query. The server opens those files with its own privileges, so accepting a path from SQL would let any user who is able to define a MySQL source probe the local filesystem, and authenticate with a certificate and key they are not allowed to read themselves.

```xml
<named_collections>
    <mysql_creds>
        <host>mysql-host</host>
        <port>3306</port>
        <user>mysql_user</user>
        <password>****</password>
        <ssl_ca>/etc/clickhouse-server/mysql-ca.crt</ssl_ca>
    </mysql_creds>
</named_collections>
```

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the `table` argument can be a `SELECT` query that is passed to MySQL as is. The structure of the table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE mysql_table ENGINE = MySQL('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

This is useful to push down joins, aggregations or any other processing to MySQL. Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`mysql`](/reference/functions/table-functions/mysql) table function.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized in the MySQL dialect (backtick identifier quoting) before being sent to the server. It must therefore be valid ClickHouse SQL. To pass MySQL-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to MySQL verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from MySQL, put the filter inside the passed query. With [`external_table_strict_query = 1`](/reference/settings/session-settings/external-table#external_table_strict_query) an outer filter on the columns of the table is rejected with an exception instead of being applied locally, because it cannot be pushed into the passed query. The check covers the top-level `WHERE` predicate and each conjunct of a top-level `AND`. A `PREWHERE` on the columns of this table is not a case for this setting: this table engine do not support `PREWHERE`, and such a query is rejected with `ILLEGAL_PREWHERE` regardless of the setting. With the analyzer (the default), the check runs only where a filter could be pushed down at all: when this table is the only table of the query, on either side of an `INNER JOIN`, or on the preserving side of an outer join (the left side of a `LEFT JOIN`, the right side of a `RIGHT JOIN`). On the non-preserving side of a `LEFT`/`RIGHT JOIN` and on either side of a `FULL JOIN` nothing is pushed down and nothing is checked, so a filter on the columns of this table is applied locally after the join even in strict mode. Where the check runs, a predicate that references other tables joined in the surrounding query is not pushed down and is excluded from the check, whether it references only the joined side or mixes it with this table inside one non-`AND` expression (for example an `OR`); such a predicate keeps its usual ClickHouse evaluation point (`WHERE` after the join, `PREWHERE` before it) and is not rejected. With the old analyzer (`enable_analyzer = 0`) this scoping does not apply: the whole outer filter is checked when this table is the first table of the join tree, including a predicate on the joined side, and a joined right-hand table is not checked.
:::

Supports multiple replicas that must be listed by `|`. For example:

```sql
CREATE TABLE test_replicas (id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL(`mysql{2|3|4}:3306`, 'clickhouse', 'test_replicas', 'root', 'clickhouse');
```

## Usage example {#usage-example}

Create table in MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

Create table in ClickHouse using plain arguments:

```sql
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL('localhost:3306', 'test', 'test', 'bayonet', '123')
```

Or using [named collections](/concepts/features/configuration/server-config/named-collections):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
CREATE TABLE mysql_table
(
    `float_nullable` Nullable(Float32),
    `int_id` Int32
)
ENGINE = MySQL(creds, table='test')
```

Retrieving data from MySQL table:

```sql
SELECT * FROM mysql_table
```

```text
┌─float_nullable─┬─int_id─┐
│           ᴺᵁᴸᴸ │      1 │
└────────────────┴────────┘
```

## Settings {#mysql-settings}

Default settings are not very efficient, since they do not even reuse connections. These settings allow you to increase the number of queries run by the server per second.

### `connection_auto_close` {#connection-auto-close}

Allows to automatically close the connection after query execution, i.e. disable connection reuse.

Possible values:

- 1 — Auto-close connection is allowed, so the connection reuse is disabled
- 0 — Auto-close connection is not allowed, so the connection reuse is enabled

Default value: `1`.

### `connection_max_tries` {#connection-max-tries}

Sets the number of retries for pool with failover.

Possible values:

- Positive integer.
- 0 — There are no retries for pool with failover.

Default value: `3`.

### `connection_pool_size` {#connection-pool-size}

Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).

Possible values:

- Positive integer.

Default value: `16`.

### `connection_wait_timeout` {#connection-wait-timeout}

Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.

Possible values:

- Positive integer.

Default value: `5`.

### `connect_timeout` {#connect-timeout}

Connect timeout (in seconds).

Possible values:

- Positive integer.

Default value: `10`.

### `read_write_timeout` {#read-write-timeout}

Read/write timeout (in seconds).

Possible values:

- Positive integer.

Default value: `300`.

### `enable_compression` {#enable-compression}

Enables compression for the MySQL protocol connection.

Default value: `false`.

This setting applies to:

- the `MySQL` table engine;
- the `MySQL` database engine;
- the `mysql` table function;
- named collections used by MySQL integrations.

When enabled, ClickHouse requests compression for the connection.

Example:

```sql
CREATE TABLE mysql_engine_compression
(
    id UInt32,
    name String,
    age UInt32,
    money UInt32
)
ENGINE = MySQL('mysql80:3306', 'clickhouse', 'test_table', 'root', 'password')
SETTINGS enable_compression = 1;
```

## See also {#see-also}

- [The mysql table function](/reference/functions/table-functions/mysql)
- [Using MySQL as a dictionary source](/reference/statements/create/dictionary/sources/mysql)
)DOCS_MD",
        .syntax = "ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, on_duplicate_clause])",
        .related = {"PostgreSQL", "SQLite", "MongoDB"}});
}

namespace
{
ColumnsDescription doQueryResultStructure(
    mysqlxx::PoolWithFailover & pool_, const String & select_query, const ContextPtr & context_,
    MultiEnum<MySQLDataTypesSupport> type_support)
{
    /// Wrap the query in a derived table and run it with `LIMIT 0` to obtain the result columns metadata
    /// without fetching any rows. The wrapping mirrors how the data is read later (see buildQueryForExternalDatabaseSubquery),
    /// so the inferred column names match the names referenced when reading.
    const auto limited_select = "SELECT * FROM (" + select_query + ") AS __subquery LIMIT 0";
    auto connection = pool_.get();
    auto query = connection->query(limited_select);
    auto query_result = query.use();

    const auto num_fields = query_result.getNumFields();
    if (num_fields == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MySQL query returned no columns: {}", select_query);

    const auto & settings = context_->getSettingsRef();
    ColumnsDescription columns;
    for (size_t i = 0; i < num_fields; ++i)
    {
        auto & field = query_result.getField(i);
        columns.add(ColumnDescription(
            query_result.getFieldName(i),
            convertMySQLDataType(
                type_support,
                field,
                settings[Setting::external_table_functions_use_nulls])));
    }

    /// Drain the (empty) result so that the connection is left in a consistent state for reuse.
    while (query_result.fetch())
        ;

    return columns;
}
}

}

#endif
