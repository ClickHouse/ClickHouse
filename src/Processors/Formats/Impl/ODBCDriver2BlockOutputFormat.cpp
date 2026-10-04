#include <Core/Block.h>
#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/ODBCDriver2BlockOutputFormat.h>
#include <Processors/Port.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeLowCardinality.h>


namespace DB
{
ODBCDriver2BlockOutputFormat::ODBCDriver2BlockOutputFormat(
    WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IOutputFormat(header_, out_), format_settings(format_settings_), serializations(header_->getSerializations())
{
}

static void writeODBCString(WriteBuffer & out, const std::string & str)
{
    writeBinaryLittleEndian(Int32(str.size()), out);
    out.write(str.data(), str.size());
}

void ODBCDriver2BlockOutputFormat::writeRow(const Columns & columns, size_t row_idx, std::string & buffer)
{
    size_t num_columns = columns.size();
    for (size_t column_idx = 0; column_idx < num_columns; ++column_idx)
    {
        buffer.clear();
        const auto & column = columns[column_idx];

        if (column->isNullAt(row_idx))
        {
            writeBinaryLittleEndian(Int32(-1), out);
        }
        else
        {
            {
                WriteBufferFromString text_out(buffer);
                serializations[column_idx]->serializeText(*column, row_idx, text_out, format_settings);
            }
            writeODBCString(out, buffer);
        }
    }
}

void ODBCDriver2BlockOutputFormat::write(Chunk chunk, PortKind)
{
    String text_value;
    const auto & columns = chunk.getColumns();

    const size_t rows = chunk.getNumRows();
    for (size_t i = 0; i < rows; ++i)
        writeRow(columns, i, text_value);
}

void ODBCDriver2BlockOutputFormat::consume(Chunk chunk)
{
    write(std::move(chunk), PortKind::Main);
}

void ODBCDriver2BlockOutputFormat::consumeTotals(Chunk chunk)
{
    write(std::move(chunk), PortKind::Totals);
}

void ODBCDriver2BlockOutputFormat::writePrefix()
{
    const auto & header = getPort(PortKind::Main).getHeader();
    const size_t columns = header.columns();

    /// Number of header rows.
    writeBinaryLittleEndian(Int32(2), out);

    /// Names of columns.
    /// Number of columns + 1 for first name column.
    writeBinaryLittleEndian(Int32(columns + 1), out);
    writeODBCString(out, "name");
    for (size_t i = 0; i < columns; ++i)
    {
        const ColumnWithTypeAndName & col = header.getByPosition(i);
        writeODBCString(out, col.name);
    }

    /// Types of columns.
    writeBinaryLittleEndian(Int32(columns + 1), out);
    writeODBCString(out, "type");
    for (size_t i = 0; i < columns; ++i)
    {
        auto type = header.getByPosition(i).type;
        if (type->lowCardinality())
            type = recursiveRemoveLowCardinality(type);
        writeODBCString(out, type->getName());
    }
}


void registerOutputFormatODBCDriver2(FormatFactory & factory);
void registerOutputFormatODBCDriver2(FormatFactory & factory)
{
    factory.registerOutputFormat(
        "ODBCDriver2", [](WriteBuffer & buf, const Block & sample, const FormatSettings & format_settings, FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<ODBCDriver2BlockOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
        });
    factory.markOutputFormatNotTTYFriendly("ODBCDriver2");
    factory.setContentType("ODBCDriver2", "application/octet-stream");

    factory.setDocumentation("ODBCDriver2", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `ODBCDriver2` format is a binary, output-only format used to transfer query results over HTTP to the
[ClickHouse driver for `ODBC`](/concepts/features/interfaces/odbc). The driver requests it by setting `default_format` to
`ODBCDriver2`; users normally don't select it directly.

All integers in the stream are signed 32-bit values encoded in little-endian byte order. A string is encoded as its byte
length followed by its bytes. The output has the following structure:

1. The value `2`, indicating that two header rows follow.
2. A names header containing the value `number_of_columns + 1`, the string `name`, and each column name.
3. A types header containing the value `number_of_columns + 1`, the string `type`, and each ClickHouse data type name.
   `LowCardinality` wrappers are removed from these type names.
4. The data rows. Each row contains one text-serialized, length-prefixed value for every result column. Data rows don't
   include their own row or column count, and the stream ends at `EOF`.

A `NULL` value is encoded with the length `-1`; an empty non-null string has length `0`. Totals, when present, use the
same row encoding and follow the ordinary rows.

## Example usage {#example-usage}

The following query is representative of the values covered by the format's tests:

```sql
SELECT
    1 AS x,
    [2, 3] AS y,
    'Hello' AS z,
    NULL AS a
FORMAT ODBCDriver2
```

Because the result is binary, direct it to a file rather than a terminal:

```shell
clickhouse-client --query \
    "SELECT 1 AS x, [2, 3] AS y, 'Hello' AS z, NULL AS a FORMAT ODBCDriver2" \
    > result.bin
```

In normal use, the driver adds `default_format=ODBCDriver2` to its HTTP requests and decodes the response.

## Format settings {#format-settings}

`ODBCDriver2` uses the standard text-serialization settings for individual values. It has no settings specific to the
format itself.
)DOCS_MD"});
}

}
