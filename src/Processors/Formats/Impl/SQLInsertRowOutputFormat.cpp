#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatFactory.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/Impl/SQLInsertRowOutputFormat.h>
#include <Processors/Port.h>


namespace DB
{

SQLInsertRowOutputFormat::SQLInsertRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), column_names(header_->getNames()), format_settings(format_settings_)
{
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
        if (format_settings.sql_insert.quote_names)
            writeChar('`', out);

        writeString(column_names[i], out);

        if (format_settings.sql_insert.quote_names)
            writeChar('`', out);

        if (i + 1 != column_names.size())
            writeCString(", ", out);
    }
    writeChar(')', out);
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

    factory.setDocumentation("SQLInsert", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

Outputs data as a sequence of `INSERT INTO table (columns...) VALUES (...), (...) ...;` statements.

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

To read data output by this format you can use [MySQLDump](../formats/MySQLDump.md) input format.

## Format settings {#format-settings}

| Setting                                                                                                                                | Description                                         | Default   |
|----------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------|-----------|
| [`output_format_sql_insert_max_batch_size`](../../operations/settings/settings-formats.md/#output_format_sql_insert_max_batch_size)    | The maximum number of rows in one INSERT statement. | `65505`   |
| [`output_format_sql_insert_table_name`](../../operations/settings/settings-formats.md/#output_format_sql_insert_table_name)            | The name of the table in the output INSERT query.   | `'table'` |
| [`output_format_sql_insert_include_column_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_include_column_names) | Include column names in INSERT query.               | `true`    |
| [`output_format_sql_insert_use_replace`](../../operations/settings/settings-formats.md/#output_format_sql_insert_use_replace)          | Use REPLACE statement instead of INSERT.            | `false`   |
| [`output_format_sql_insert_quote_names`](../../operations/settings/settings-formats.md/#output_format_sql_insert_quote_names)          | Quote column names with "\`" characters.            | `true`    |
)DOCS_MD"});
}


}
