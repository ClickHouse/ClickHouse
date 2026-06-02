#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/StorageMySQL.h>

#if USE_MYSQL

#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTCreateQuery.h>
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
    extern const SettingsUInt64 glob_expansion_max_elements;
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
    extern const SettingsUInt64 mysql_max_rows_to_insert;
}

namespace MySQLSetting
{
    extern const MySQLSettingsBool connection_auto_close;
    extern const MySQLSettingsUInt64 connection_pool_size;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
}

StorageMySQL::StorageMySQL(
    const StorageID & table_id_,
    mysqlxx::PoolWithFailover && pool_,
    const std::string & remote_database_name_,
    const std::string & remote_table_name_,
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
    , remote_table_name(remote_table_name_)
    , replace_query{replace_query_}
    , on_duplicate_clause{on_duplicate_clause_}
    , mysql_settings(std::make_unique<MySQLSettings>(mysql_settings_))
    , pool(std::make_shared<mysqlxx::PoolWithFailover>(pool_))
    , log(getLogger("StorageMySQL (" + table_id_.getFullTableName() + ")"))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(*pool, remote_database_name, remote_table_name, context_);
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
    const String & table,
    const ContextPtr & context_)
{
    const auto & settings = context_->getSettingsRef();
    const auto tables_and_columns = fetchTablesColumnsList(pool_, database, {table}, settings, settings[Setting::mysql_datatypes_support_level]);

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
    String query = transformQueryForExternalDatabase(
        query_info,
        column_names,
        storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::BackticksMySQL,
        LiteralEscapingStyle::Regular,
        remote_database_name,
        remote_table_name,
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

    StreamSettings mysql_input_stream_settings(context_->getSettingsRef(),
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
    return std::make_shared<StorageMySQLSink>(
        *this,
        metadata_snapshot,
        remote_database_name,
        remote_table_name,
        pool->get(),
        local_context->getSettingsRef()[Setting::mysql_max_rows_to_insert]);
}

StorageMySQL::Configuration StorageMySQL::processNamedCollectionResult(
    const NamedCollection & named_collection, MySQLSettings & storage_settings, ContextPtr context_, bool require_table)
{
    StorageMySQL::Configuration configuration;

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> optional_arguments = {"replace_query", "on_duplicate_clause", "addresses_expr", "host", "hostname", "port", "ssl_ca", "ssl_cert", "ssl_key"};
    auto mysql_settings_names = storage_settings.getAllRegisteredNames();
    for (const auto & name : mysql_settings_names)
        optional_arguments.insert(name);

    ValidateKeysMultiset<ExternalDatabaseEqualKeysSet> required_arguments = {"user", "username", "password", "database", "db"};
    if (require_table)
        required_arguments.insert("table");
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
    if (require_table)
        configuration.table = named_collection.get<String>("table");
    configuration.replace_query = named_collection.getOrDefault<UInt64>("replace_query", false);
    configuration.on_duplicate_clause = named_collection.getOrDefault<String>("on_duplicate_clause", "");
    configuration.ssl_ca = named_collection.getOrDefault<String>("ssl_ca", "");
    configuration.ssl_cert = named_collection.getOrDefault<String>("ssl_cert", "");
    configuration.ssl_key = named_collection.getOrDefault<String>("ssl_key", "");

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
        if (engine_args.size() < 5 || engine_args.size() > 7)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Storage MySQL requires 5-7 parameters: "
                            "MySQL('host:port' (or 'addresses_pattern'), database, table, "
                            "'user', 'password'[, replace_query, 'on_duplicate_clause']).");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, context_);

        configuration.addresses_expr = checkAndGetLiteralArgument<String>(engine_args[0], "host:port");
        size_t max_addresses = context_->getSettingsRef()[Setting::glob_expansion_max_elements];

        configuration.addresses = parseRemoteDescriptionForExternalDatabase(configuration.addresses_expr, max_addresses, 3306);
        configuration.database = checkAndGetLiteralArgument<String>(engine_args[1], "database");
        configuration.table = checkAndGetLiteralArgument<String>(engine_args[2], "table");
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
    const StreamSettings & mysql_input_stream_settings_
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

        if (args.storage_def->settings)
            mysql_settings.loadFromQuery(*args.storage_def);

        if (!mysql_settings[MySQLSetting::connection_pool_size])
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "connection_pool_size cannot be zero.");

        mysqlxx::PoolWithFailover pool = createMySQLPoolWithFailover(configuration, mysql_settings);

        return std::make_shared<StorageMySQL>(
            args.table_id,
            std::move(pool),
            configuration.database,
            configuration.table,
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
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = MySQL({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
SETTINGS
    [ connection_pool_size=16, ]
    [ connection_max_tries=3, ]
    [ connection_wait_timeout=5, ]
    [ connection_auto_close=true, ]
    [ connect_timeout=10, ]
    [ read_write_timeout=300 ]
;
```

See a detailed description of the [CREATE TABLE](/sql-reference/statements/create/table) query.

The table structure can differ from the original MySQL table structure:

- Column names should be the same as in the original MySQL table, but you can use just some of these columns and in any order.
- Column types may differ from those in the original MySQL table. ClickHouse tries to [cast](../../../engines/database-engines/mysql.md#data_types-support) values to the ClickHouse data types.
- The [external_table_functions_use_nulls](/operations/settings/settings#external_table_functions_use_nulls) setting defines how to handle Nullable columns. Default value: 1. If 0, the table function does not make Nullable columns and inserts default values instead of nulls. This is also applicable for NULL values inside arrays.

**Engine Parameters**

- `host:port` — MySQL server address.
- `database` — Remote database name.
- `table` — Remote table name.
- `user` — MySQL user.
- `password` — User password.
- `replace_query` — Flag that converts `INSERT INTO` queries to `REPLACE INTO`. If `replace_query=1`, the query is substituted.
- `on_duplicate_clause` — The `ON DUPLICATE KEY on_duplicate_clause` expression that is added to the `INSERT` query.
    Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See the [MySQL documentation](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause.
    To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

Arguments also can be passed using [named collections](/operations/named-collections.md). In this case `host` and `port` should be specified separately. This approach is recommended for production environment.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

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

Or using [named collections](/operations/named-collections.md):

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

## See also {#see-also}

- [The mysql table function](../../../sql-reference/table-functions/mysql.md)
- [Using MySQL as a dictionary source](/sql-reference/statements/create/dictionary/sources/mysql)
)DOCS_MD",
        .syntax = "ENGINE = MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, on_duplicate_clause])",
        .related = {"PostgreSQL", "SQLite", "MongoDB"}});
}

}

#endif
