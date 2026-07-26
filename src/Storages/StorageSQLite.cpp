#include <Storages/StorageSQLite.h>

#if USE_SQLITE
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <Processors/Sources/SQLiteSource.h>
#include <Databases/SQLite/SQLiteUtils.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>
#include <DataTypes/DataTypeLowCardinality.h>
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
#include <Storages/transformQueryForExternalDatabase.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <QueryPipeline/Pipe.h>
#include <Common/filesystemHelpers.h>

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
}

StorageSQLite::StorageSQLite(
    const StorageID & table_id_,
    SQLitePtr sqlite_db_,
    const String & database_path_,
    const String & remote_table_name_,
    const ColumnsDescription & columns_,
    const ConstraintsDescription & constraints_,
    const String & comment,
    ContextPtr context_)
    : StorageWithCommonVirtualColumns(table_id_)
    , WithContext(context_->getGlobalContext())
    , remote_table_name(remote_table_name_)
    , database_path(database_path_)
    , sqlite_db(sqlite_db_)
    , log(getLogger("StorageSQLite (" + table_id_.getFullTableName() + ")"))
    , write_context(makeSQLiteWriteContext(getContext()))
{
    StorageInMemoryMetadata storage_metadata;

    if (columns_.empty())
    {
        auto columns = getTableStructureFromData(sqlite_db, remote_table_name);
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
    const String & table)
{
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

    String query = transformQueryForExternalDatabase(
        query_info,
        column_names,
        storage_snapshot->metadata->getColumns().getOrdinary(),
        IdentifierQuotingStyle::DoubleQuotes,
        LiteralEscapingStyle::Regular,
        "",
        remote_table_name,
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
    if (!sqlite_db)
        sqlite_db = openSQLiteDB(database_path, getContext(), /* throw_on_error */true);
    return std::make_shared<SQLiteSink>(*this, metadata_snapshot, sqlite_db, remote_table_name);
}


void registerStorageSQLite(StorageFactory & factory);
void registerStorageSQLite(StorageFactory & factory)
{
    factory.registerStorage("SQLite", [](const StorageFactory::Arguments & args) -> StoragePtr
    {
        ASTs & engine_args = args.engine_args;

        if (engine_args.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SQLite database requires 2 arguments: database path, table name");

        for (auto & engine_arg : engine_args)
            engine_arg = evaluateConstantExpressionOrIdentifierAsLiteral(engine_arg, args.getLocalContext());

        const auto database_path = checkAndGetLiteralArgument<String>(engine_args[0], "database_path");
        const auto table_name = checkAndGetLiteralArgument<String>(engine_args[1], "table_name");

        auto sqlite_db = openSQLiteDB(database_path, args.getContext(), /* throw_on_error */ args.mode <= LoadingStrictnessLevel::CREATE);

        return std::make_shared<StorageSQLite>(args.table_id, sqlite_db, database_path,
                                     table_name, args.columns, args.constraints, args.comment, args.getContext());
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
- `table` — Name of a table in the SQLite database.

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

}

#endif
