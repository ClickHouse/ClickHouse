#include <Storages/StorageSQLite.h>

#if USE_SQLITE
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Processors/Sources/SQLiteSource.h>
#include <Databases/SQLite/SQLiteUtils.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/IOutputFormat.h>
#include <IO/Operators.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/TableNameOrQuery.h>
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <QueryPipeline/Pipe.h>
#include <Common/filesystemHelpers.h>
#include <base/scope_guard.h>

namespace
{

using namespace DB;

ContextPtr makeSQLiteWriteContext(ContextPtr context)
{
    auto write_context = Context::createCopy(context);
    write_context->setSetting("output_format_values_escape_quote_with_quote", Field(true));
    return write_context;
}

}


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SQLITE_ENGINE_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{
/// Infer the structure of a result of a user-provided query by preparing it and reading the result columns metadata.
ColumnsDescription doQueryResultStructure(sqlite3 * sqlite_db, const String & query);
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    SQLitePtr sqlite_db_,
    const String & database_path_,
    const TableNameOrQuery & remote_table_or_query_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_)
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_or_query(remote_table_or_query_)
    , database_path(database_path_)
    , sqlite_db(sqlite_db_)
    , log(getLogger("StorageSQLite (" + table_id_.getFullTableName() + ")"))
    , write_context(makeSQLiteWriteContext(getContext()))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(sqlite_db, remote_table_or_query);
        storage_metadata.setColumns(columns);
    }
    else
        storage_metadata.setColumns(columns_);

    storage_metadata.setConstraints(constraints_);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);
}

VirtualColumnsDescription StorageSQLite::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}


ColumnsDescription StorageSQLite::getTableStructureFromData(
    const SQLitePtr & sqlite_db_,
    const TableNameOrQuery & table_or_query)
{
    if (table_or_query.isQuery())
        return doQueryResultStructure(sqlite_db_.get(), table_or_query.getQuery());

    const auto & table = table_or_query.getTableName();
    auto columns = fetchSQLiteTableStructure(sqlite_db_.get(), table);

    if (!columns)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Failed to fetch table structure for {}", table);

    return ColumnsDescription{*columns};
}


Pipe StorageSQLite::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context_,
    QueryProcessingStage::Enum,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);

    storage_snapshot->check(column_names);

    String query;
    if (remote_table_or_query.isQuery())
    {
        /// The user-provided query is passed to SQLite as is; no outer predicate is pushed down into it, so
        /// reject any outer filter under external_table_strict_query.
        rejectOuterFilterForQueryBackedExternalSourceIfStrict(query_info, context_);
        query = buildQueryForExternalDatabaseSubquery(remote_table_or_query.getQuery(), column_names, IdentifierQuotingStyle::DoubleQuotes);
    }
    else
        query = transformQueryForExternalDatabase(
            query_info,
            column_names,
            storage_snapshot->metadata->getColumns().getOrdinary(),
            IdentifierQuotingStyle::DoubleQuotes,
            LiteralEscapingStyle::Regular,
            "",
            remote_table_or_query.getTableName(),
            context_);
    LOG_TRACE(log, "Query: {}", query);

    Block sample_block;
    for (const String & column_name : column_names)
    {
        auto column_data = storage_snapshot->metadata->getColumns().getPhysical(column_name);
        sample_block.insert({column_data.type, column_data.name});
    }

    return Pipe(std::make_shared<SQLiteSource>(sqlite_db, query, sample_block, max_block_size));
}


class SQLiteSink final : public SinkToStorage
{
public:
    explicit SQLiteSink(
        const StorageSQLite & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        StorageSQLite::SQLitePtr sqlite_db_,
        const String & remote_table_name_)
        : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
        , storage{storage_}
        , metadata_snapshot(metadata_snapshot_)
        , sqlite_db(sqlite_db_)
        , remote_table_name(remote_table_name_)
    {
    }

    String getName() const override { return "SQLiteSink"; }

    void consume(Chunk & chunk) override
    {
        auto block = getHeader().cloneWithColumns(chunk.getColumns());
        WriteBufferFromOwnString sqlbuf;

        sqlbuf << "INSERT INTO ";
        sqlbuf << doubleQuoteString(remote_table_name);
        sqlbuf << " (";

        for (auto it = block.begin(); it != block.end(); ++it)
        {
            if (it != block.begin())
                sqlbuf << ", ";
            sqlbuf << quoteString(it->name);
        }

        sqlbuf << ") VALUES ";

        auto writer = FormatFactory::instance().getOutputFormat("Values", sqlbuf, metadata_snapshot->getSampleBlock(), storage.write_context);
        writer->write(block);

        sqlbuf << ";";

        char * err_message = nullptr;
        int status = sqlite3_exec(sqlite_db.get(), sqlbuf.str().c_str(), nullptr, nullptr, &err_message);

        if (status != SQLITE_OK)
        {
            String err_msg(err_message);
            sqlite3_free(err_message);
            throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                            "Failed to execute sqlite INSERT query. Status: {}. Message: {}",
                            status, err_msg);
        }
    }

private:
    const StorageSQLite & storage;
    StorageMetadataPtr metadata_snapshot;
    StorageSQLite::SQLitePtr sqlite_db;
    String remote_table_name;
};


SinkToStoragePtr StorageSQLite::write(const ASTPtr & /* query */, const StorageMetadataPtr & metadata_snapshot, ContextPtr /*context*/, bool /*async_insert*/)
{
    if (remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Cannot write into a SQLite table representing the result of a query");

    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);
    return std::make_shared<SQLiteSink>(*this, metadata_snapshot, sqlite_db, remote_table_or_query.getTableName());
}


void registerStorageSQLite(StorageFactory & factory);
void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage("SQLite", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SQLite database requires 2 arguments: database path, table name (or query)");

        /// The 2nd argument is either a table name, or a query passed to SQLite as is - `(SELECT ...)` or `query('SELECT ...')`.
        auto maybe_query = tryGetExternalDatabaseQuery(
            engine_args[1], args.getLocalContext(), IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::Regular);
        for (size_t i = 0; i < engine_args.size(); ++i)
        {
            if (i == 1 && maybe_query)
                continue;
            engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(engine_args[i], args.getLocalContext());
        }

        const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
        TableNameOrQuery table_or_query = maybe_query
            ? TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query)
            : TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(engine_args[1], "table_name"));

        auto sqlite_db = openSQLiteDB(database_path, args.getContext(), /* throw_on_error */ args.mode <= LoadingStrictnessLevel::CREATE);

        return std::make_shared<StorageSQLite>(args.table_id, sqlite_db, database_path,
                                     table_or_query, args.columns, args.constraints, args.comment, args.getContext());
    },
    {
        .supports_schema_inference = true,
        .source_access_type = AccessTypeObjects::Source::SQLITE,
    },
    Documentation{
        .description = R"DOCS_MD(
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# SQLite table engine

<CloudNotSupportedBadge/>

The engine allows to import and export data to SQLite and supports queries to SQLite tables directly from ClickHouse.

## Creating a table {#creating-a-table}

```sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**Engine Parameters**

- `db_path` — Path to SQLite file with a database.
- `table` — Name of a table in the SQLite database, or a query passed to SQLite as is (see [Passing a query instead of a table name](#passing-a-query)).

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the `table` argument can be a `SELECT` query that is passed to SQLite as is. The structure of the table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
CREATE TABLE sqlite_table ENGINE = SQLite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`sqlite`](/sql-reference/table-functions/sqlite) table function.

:::note
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized before being sent to SQLite. It must therefore be valid ClickHouse SQL. To pass SQLite-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to SQLite verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from SQLite, put the filter inside the passed query. With [`external_table_strict_query = 1`](/operations/settings/settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
:::

## Data types support {#data-types-support}

When you explicitly specify ClickHouse column types in the table definition, the following ClickHouse types can be parsed from SQLite TEXT columns:

- [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md)
- [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md)
- [UUID](../../../sql-reference/data-types/uuid.md)
- [Enum8, Enum16](../../../sql-reference/data-types/enum.md)
- [Decimal32, Decimal64, Decimal128, Decimal256](../../../sql-reference/data-types/decimal.md)
- [FixedString](../../../sql-reference/data-types/fixedstring.md)
- All integer types ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../../sql-reference/data-types/int-uint.md))
- [Float32, Float64](../../../sql-reference/data-types/float.md)

See [SQLite database engine](../../../engines/database-engines/sqlite.md#data_types-support) for the default type mapping.

## Usage example {#usage-example}

Shows a query creating the SQLite table:

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

```text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

Returns the data from the table:

```sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**See Also**

- [SQLite](../../../engines/database-engines/sqlite.md) engine
- [sqlite](../../../sql-reference/table-functions/sqlite.md) table function
)DOCS_MD",
        .syntax = "ENGINE = SQLite('path_to_database_file', 'table')",
        .related = {"MySQL", "PostgreSQL"}});
}

namespace
{
ColumnsDescription doQueryResultStructure(sqlite3 * sqlite_db, const String & query)
{
    /// Wrap the query into a subquery (mirroring how the data is read) and prepare it to read the result
    /// columns metadata without executing it. SQLite is dynamically typed, so column types are inferred from
    /// the declared types of the underlying table columns; expression columns fall back to Nullable(String).
    const auto wrapped = "SELECT * FROM (" + query + ") AS __subquery";

    sqlite3_stmt * compiled_stmt = nullptr;
    int status = sqlite3_prepare_v2(sqlite_db, wrapped.c_str(), static_cast<int>(wrapped.size() + 1), &compiled_stmt, nullptr);
    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Cannot prepare SQLite query. Status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    SCOPE_EXIT({ sqlite3_finalize(compiled_stmt); });

    const int column_count = sqlite3_column_count(compiled_stmt);
    if (column_count == 0)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "SQLite query returned no columns: {}", query);

    NamesAndTypesList columns;
    for (int i = 0; i < column_count; ++i)
    {
        const char * name = sqlite3_column_name(compiled_stmt, i);
        const char * decl_type = sqlite3_column_decltype(compiled_stmt, i);

        DataTypePtr type = decl_type ? convertSQLiteDataType(decl_type) : std::make_shared<DataTypeString>();
        columns.emplace_back(String(name), std::make_shared<DataTypeNullable>(type));
    }

    return ColumnsDescription{columns};
}
}

}

#endif
