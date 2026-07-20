#include "config.h"

#if USE_SQLITE

#    include <Databases/SQLite/fetchSQLiteTableStructure.h>
#    include <Databases/SQLite/SQLiteUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/SchemaInferenceUtils.h>
#    include <IO/WriteBufferFromString.h>
#    include <IO/WriteHelpers.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/IRowInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <Processors/Sources/SQLiteStatementReader.h>
#    include <Processors/Formats/Impl/SQLiteCommon.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SQLITE_ENGINE_ERROR;
}

namespace
{

using namespace SQLiteFormatImpl;

String makeSelectQuery(const Block & header, const String & table_name)
{
    WriteBufferFromOwnString query;
    writeCString("SELECT ", query);

    for (size_t i = 0; i != header.columns(); ++i)
    {
        if (i)
            writeCString(", ", query);
        writeString(quoteSQLiteIdentifier(header.getByPosition(i).name), query);
    }

    writeCString(" FROM ", query);
    writeString(quoteSQLiteIdentifier(table_name), query);
    return query.str();
}

String resolveInputTableName(sqlite3 * db, const FormatSettings & settings)
{
    if (!settings.sqlite.input_table_name.empty())
        return settings.sqlite.input_table_name;

    auto statement = prepareSQLiteStatement(
        db,
        "SELECT name FROM sqlite_master "
        "WHERE type = 'table' AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
        "ORDER BY rowid LIMIT 1");

    int status = sqlite3_step(statement.get());
    if (status == SQLITE_DONE)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot find any table in SQLite database");

    checkSQLiteStatus(db, status, "Cannot fetch first SQLite table name");

    const char * data = reinterpret_cast<const char *>(sqlite3_column_text(statement.get(), 0));
    int size = sqlite3_column_bytes(statement.get(), 0);
    if (!data && size)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot read first SQLite table name");

    return String(data ? data : "", static_cast<size_t>(size));
}

class SQLiteInputFormat final : public IInputFormat
{
public:
    SQLiteInputFormat(ReadBuffer & in_, SharedHeader header_, const FormatSettings & settings_, UInt64 max_block_size_)
        : IInputFormat(header_, &in_)
        , header(std::move(header_))
        , settings(settings_)
        , max_block_size(max_block_size_)
        , statement_reader(*header, settings, SQLiteStatementReader::ValueReadMode::Text)
    {
    }

    String getName() const override { return "SQLite"; }

    Chunk read() override
    {
        if (sqlite_finished)
            return {};

        if (!initialized)
            initialize();

        bool finished = false;
        auto chunk = statement_reader.readChunk(sqlite_db.get(), statement.get(), max_block_size, finished, [this] { return isCancelled(); });
        if (finished)
        {
            statement.reset();
            sqlite_finished = true;
        }

        return chunk;
    }

private:
    void initialize()
    {
        sqlite_db = openSQLiteDatabaseForRead(*in, settings);
        const auto table_name = resolveInputTableName(sqlite_db.get(), settings);
        statement = prepareSQLiteStatement(sqlite_db.get(), makeSelectQuery(*header, table_name));
        initialized = true;
    }

    SharedHeader header;
    FormatSettings settings;
    UInt64 max_block_size;
    SQLiteStatementReader statement_reader;
    SQLiteDatabase sqlite_db;
    SQLiteStatementPtr statement{nullptr, sqlite3_finalize};
    bool initialized = false;
    bool sqlite_finished = false;
};

class SQLiteSchemaReader final : public ISchemaReader
{
public:
    SQLiteSchemaReader(ReadBuffer & in_, const FormatSettings & settings_)
        : ISchemaReader(in_)
        , settings(settings_)
    {
    }

    NamesAndTypesList readSchema() override
    {
        auto db = openSQLiteDatabaseForRead(in, settings);
        auto table_name = resolveInputTableName(db.get(), settings);
        auto columns = fetchSQLiteTableStructure(db.get(), table_name);

        if (!columns)
            throw Exception(
                ErrorCodes::SQLITE_ENGINE_ERROR, "Cannot fetch table structure for SQLite table {}", table_name);

        /// `getAll` keeps every column `SELECT *` returns (including generated columns, which the structure
        /// marks `MATERIALIZED`) in declaration order, so schema inference exposes exactly the readable columns.
        auto names_and_types = columns->getAll();

        /// `fetchSQLiteTableStructure` reports nullability from SQLite metadata (the `NOT NULL` constraint).
        /// Honor `schema_inference_make_columns_nullable` like other metadata-backed formats (e.g. Parquet, ORC):
        /// 0 - never `Nullable`, 1 - always `Nullable`, otherwise keep the nullability from metadata.
        if (settings.schema_inference_make_columns_nullable == 0 || settings.schema_inference_make_columns_nullable == 1)
        {
            NamesAndTypesList result;
            for (const auto & name_and_type : names_and_types)
            {
                auto type = settings.schema_inference_make_columns_nullable == 1
                    ? makeNullableRecursively(name_and_type.type, settings)
                    : removeNullableRecursively(name_and_type.type, settings);
                result.emplace_back(name_and_type.name, type);
            }
            return result;
        }

        return names_and_types;
    }

private:
    FormatSettings settings;
};

}

void registerInputFormatSQLite(FormatFactory & factory);
void registerInputFormatSQLite(FormatFactory & factory)
{
    factory.registerInputFormat(
        "SQLite",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams & params, const FormatSettings & settings)
        { return std::make_shared<SQLiteInputFormat>(buf, std::make_shared<const Block>(sample), settings, params.max_block_size_rows); });

    factory.markFormatSupportsSubsetOfColumns("SQLite");
    factory.registerFileExtension("sqlite", "SQLite");
    factory.registerFileExtension("sqlite3", "SQLite");

    factory.setDocumentation("SQLite", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

The `SQLite` format reads and writes a SQLite database file.

On output, ClickHouse writes the query result into a single table in the SQLite database. On input, ClickHouse reads a single table from the SQLite database.

For local regular files, ClickHouse opens the SQLite database file directly. For other input and output streams, ClickHouse materializes the SQLite database in memory.

On input, ClickHouse reads the first table from the SQLite database by default. You can change it with the `input_format_sqlite_table_name` setting. On output, the default table name is `table`; you can change it with the `output_format_sqlite_table_name` setting.

## Example usage {#example-usage}

```bash
clickhouse-client --query="SELECT number, toString(number) AS s FROM numbers(10) FORMAT SQLite" > data.sqlite
clickhouse-local --input-format SQLite --structure "number UInt64, s String" --query "SELECT * FROM table" < data.sqlite
```

## Data types matching {#data-types-matching}

When ClickHouse writes data in the `SQLite` format, it creates a SQLite table with the following declared types:

| ClickHouse data type | SQLite declared type |
|----------------------|----------------------|
| `UInt64`, `Int128`, `UInt128`, `Int256`, `UInt256` | `TEXT` |
| `Bool`, `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32` | `INTEGER` |
| Floating-point types | `REAL` |
| Other types | `TEXT` |

For `Float32` and `Float64` columns, non-`NaN` values are written using SQLite native storage classes, while `NaN` values are written using ClickHouse text serialization. `Bool` and ordinary integer values are also written using SQLite native storage classes. Wide integers and complex types are written using ClickHouse text serialization. `NULL` values are written as SQLite `NULL`.

When ClickHouse infers a schema from SQLite input, it uses the same mapping as the [SQLite database engine](/engines/database-engines/sqlite#data_types-support):

| SQLite declared type | ClickHouse inferred type |
|----------------------|--------------------------|
| Type name contains `INT` | `Int64` |
| `REAL`, `FLOAT`, `DOUBLE` | `Float64` |
| Other types, including `TEXT` and `BLOB` | `String` |

If a column is nullable in SQLite, ClickHouse wraps the inferred type in `Nullable`.

Schema inference does not preserve the original ClickHouse data types. For example, `Bool` is written as SQLite `INTEGER` and inferred as `Int64`, while `Date`, `DateTime`, `Decimal`, `UUID`, `IPv4`, `IPv6`, `Enum`, `Array`, `Tuple`, and `Map` are written as SQLite `TEXT` and inferred as `String`.

When the ClickHouse table structure is specified explicitly, floating-point values are read as SQLite `REAL` values, with a text fallback for `NaN`. Other SQLite values are read as text and parsed into the requested ClickHouse types.

## Format settings {#format-settings}

| Setting                                                                                                                     | Description                                      | Default   |
|-----------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------|-----------|
| [`input_format_sqlite_table_name`](/operations/settings/settings-formats#input_format_sqlite_table_name)           | The name of the table to read from SQLite input. If empty, the first table is used. | `''` |
| [`output_format_sqlite_table_name`](/operations/settings/settings-formats#output_format_sqlite_table_name)         | The name of the table in SQLite output.          | `'table'` |
)DOCS_MD"});
}

void registerSQLiteSchemaReader(FormatFactory & factory);
void registerSQLiteSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "SQLite", [](ReadBuffer & buf, const FormatSettings & settings) { return std::make_shared<SQLiteSchemaReader>(buf, settings); });

    factory.registerAdditionalInfoForSchemaCacheGetter("SQLite", [](const FormatSettings & settings)
    {
        return fmt::format(
            "input_table_name={}, schema_inference_make_columns_nullable={}",
            settings.sqlite.input_table_name,
            settings.schema_inference_make_columns_nullable);
    });
}

}

#endif
