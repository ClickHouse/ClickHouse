#include <Core/Names.h>
#include <Core/Settings.h>
#include <Common/isValidUTF8.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Processors/Formats/Impl/SQLInsertRowOutputFormat.h>
#include <Processors/Port.h>
#include <Storages/StorageFactory.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DUPLICATE_COLUMN;
}

namespace
{

const DataTypeValidationSettings & getDefaultDataTypeValidationSettings()
{
    static const DataTypeValidationSettings settings = []
    {
        Settings default_settings;
        return DataTypeValidationSettings(default_settings);
    }();

    return settings;
}

}

SQLInsertRowOutputFormat::SQLInsertRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), column_names(header_->getNames()), format_settings(format_settings_)
{
    if (format_settings.sql_insert.include_table_schema && format_settings.sql_insert.use_replace)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Settings `output_format_sql_insert_include_table_schema` and `output_format_sql_insert_use_replace` cannot be enabled at the same time");

    if (format_settings.sql_insert.include_table_schema)
    {
        NameSet unique_column_names;
        for (const auto & column_name : column_names)
        {
            if (!unique_column_names.emplace(column_name).second)
                throw Exception(ErrorCodes::DUPLICATE_COLUMN, "Column {} already exists", backQuoteIfNeed(column_name));
        }

        for (const auto & type : types)
            validateDataType(type, getDefaultDataTypeValidationSettings());

        checkAllTypesAreAllowedInTable(header_->getNamesAndTypesList());
    }
}

void SQLInsertRowOutputFormat::writePrefix()
{
    if (!format_settings.sql_insert.include_table_schema)
        return;

    writeCString("CREATE TABLE ", out);
    writeString(format_settings.sql_insert.table_name, out);
    writeCString("\n(\n", out);

    for (size_t i = 0; i != column_names.size(); ++i)
    {
        writeCString("    ", out);
        printColumnName(column_names[i]);
        writeChar(' ', out);
        writeString(types[i]->getName(), out);

        if (i + 1 != column_names.size())
            writeChar(',', out);

        writeChar('\n', out);
    }

    writeCString(")\nENGINE = MergeTree\nORDER BY tuple();\n", out);
}

void SQLInsertRowOutputFormat::writeRowStartDelimiter()
{
    if (rows_in_line == 0)
        printLineStart();
    writeChar('(', out);
}

void SQLInsertRowOutputFormat::printLineStart()
{
    if (format_settings.sql_insert.use_replace)
        writeCString("REPLACE INTO ", out);
    else
        writeCString("INSERT INTO ", out);

    writeString(format_settings.sql_insert.table_name, out);

    if (format_settings.sql_insert.include_column_names)
        printColumnNames();

    writeCString(" VALUES ", out);
}

void SQLInsertRowOutputFormat::printColumnNames()
{
    writeCString(" (", out);
    for (size_t i = 0; i != column_names.size(); ++i)
    {
        printColumnName(column_names[i]);

        if (i + 1 != column_names.size())
            writeCString(", ", out);
    }
    writeChar(')', out);
}

void SQLInsertRowOutputFormat::printColumnName(const String & column_name)
{
    /// Schema output must remain replayable even when a column name is not a valid unquoted identifier.
    if (format_settings.sql_insert.quote_names || format_settings.sql_insert.include_table_schema)
        writeBackQuotedString(column_name, out);
    else
        writeString(column_name, out);
}

void SQLInsertRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextQuoted(column, row_num, out, format_settings);
}

void SQLInsertRowOutputFormat::writeFieldDelimiter()
{
    writeCString(", ", out);
}

void SQLInsertRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(')', out);
    ++rows_in_line;
}

void SQLInsertRowOutputFormat::writeRowBetweenDelimiter()
{
    if (rows_in_line >= format_settings.sql_insert.max_batch_size)
    {
        writeCString(";\n", out);
        rows_in_line = 0;
    }
    else
    {
        writeCString(", ", out);
    }
}

void SQLInsertRowOutputFormat::writeSuffix()
{
    if (haveWrittenData() || !format_settings.sql_insert.include_table_schema)
        writeCString(";\n", out);
}

void SQLInsertRowOutputFormat::resetFormatterImpl()
{
    rows_in_line = 0;
}

void registerOutputFormatSQLInsert(FormatFactory & factory);
void registerOutputFormatSQLInsert(FormatFactory & factory)
{
    factory.registerOutputFormat("SQLInsert", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        return std::make_shared<SQLInsertRowOutputFormat>(buf, std::make_shared<const Block>(sample), settings);
    });

    factory.setContentType("SQLInsert", "text/plain; charset=UTF-8");

    /// `output_format_sql_insert_table_name`, column names, and (with
    /// `output_format_sql_insert_include_table_schema`) data type names are written verbatim, so a value
    /// that is not valid UTF-8 makes the output non-textual. Quoted identifiers and names of `Enum`
    /// elements can contain arbitrary bytes. All are knowable before any row is written (from the
    /// settings and the header), so they are detected here and the text framings reject or base64-encode
    /// the output accordingly.
    factory.registerOutputFormatMayProduceRawBytesChecker("SQLInsert", [](const FormatSettings & settings, const Block & header)
    {
        auto is_not_valid_utf8 = [](const std::string & s)
        {
            return !UTF8::isValidUTF8(reinterpret_cast<const UInt8 *>(s.data()), s.size());
        };

        if (is_not_valid_utf8(settings.sql_insert.table_name))
            return true;

        /// `output_format_sql_insert_include_table_schema` writes the column names in the
        /// `CREATE TABLE` statement even when `output_format_sql_insert_include_column_names` is disabled.
        if (settings.sql_insert.include_column_names || settings.sql_insert.include_table_schema)
        {
            for (const auto & column_name : header.getNames())
                if (is_not_valid_utf8(column_name))
                    return true;
        }

        if (settings.sql_insert.include_table_schema)
        {
            for (const auto & type : header.getDataTypes())
                if (is_not_valid_utf8(type->getName()))
                    return true;
        }

        return false;
    });

    factory.setDocumentation("SQLInsert", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Outputs data as a sequence of `INSERT INTO table (columns...) VALUES (...), (...) ...;` statements.
Optionally, it can prepend a `CREATE TABLE` statement containing the result column names and types.

## Example usage {#example-usage}

Example:

```sql
SELECT number AS x, number + 1 AS y, 'Hello' AS z FROM numbers(10) FORMAT SQLInsert SETTINGS output_format_sql_insert_max_batch_size = 2
```

```sql
INSERT INTO table (x, y, z) VALUES (0, 1, 'Hello'), (1, 2, 'Hello');
INSERT INTO table (x, y, z) VALUES (2, 3, 'Hello'), (3, 4, 'Hello');
INSERT INTO table (x, y, z) VALUES (4, 5, 'Hello'), (5, 6, 'Hello');
INSERT INTO table (x, y, z) VALUES (6, 7, 'Hello'), (7, 8, 'Hello');
INSERT INTO table (x, y, z) VALUES (8, 9, 'Hello'), (9, 10, 'Hello');
```

To include a table definition in the output:

```sql
SELECT number AS x, toString(number) AS y FROM numbers(2)
FORMAT SQLInsert
SETTINGS output_format_sql_insert_include_table_schema = 1, output_format_sql_insert_table_name = 'test'
```

```sql
CREATE TABLE test
(
    `x` UInt64,
    `y` String
)
ENGINE = MergeTree
ORDER BY tuple();
INSERT INTO test (`x`, `y`) VALUES (0, '0'), (1, '1');
```

The generated table definition describes the query result. It does not preserve source table metadata such as keys, default expressions, codecs, TTLs, or indexes.

Output without the table definition can be read using the [MySQLDump](/reference/formats/MySQLDump) input format.
Output with the table definition can be executed as a ClickHouse SQL script instead.
`output_format_sql_insert_include_table_schema` and `output_format_sql_insert_use_replace` cannot be enabled at the same time.

## Format settings {#format-settings}

| Setting                                                                                                                                | Description                                         | Default   |
|----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|-----------|
| [`output_format_sql_insert_max_batch_size`](/reference/settings/formats/output-format#output_format_sql_insert_max_batch_size)    | The maximum number of rows in one INSERT statement. | `65505`   |
| [`output_format_sql_insert_table_name`](/reference/settings/formats/output-format#output_format_sql_insert_table_name)            | The name of the table in the output INSERT query.   | `'table'` |
| [`output_format_sql_insert_include_column_names`](/reference/settings/formats/output-format#output_format_sql_insert_include_column_names) | Include column names in INSERT query.               | `true`    |
| [`output_format_sql_insert_use_replace`](/reference/settings/formats/output-format#output_format_sql_insert_use_replace)          | Use REPLACE statement instead of INSERT.            | `false`   |
| [`output_format_sql_insert_quote_names`](/reference/settings/formats/output-format#output_format_sql_insert_quote_names)          | Quote column names with "\`" characters. Schema output always quotes column names. | `true` |
| [`output_format_sql_insert_include_table_schema`](/reference/settings/formats/output-format#output_format_sql_insert_include_table_schema) | Include a `CREATE TABLE` statement before the data. | `false`   |
)DOCS_MD"});
}


}
