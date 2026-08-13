#include <Formats/FormatFactory.h>
#include <Processors/Formats/Impl/RawBLOBRowInputFormat.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

RawBLOBRowInputFormat::RawBLOBRowInputFormat(SharedHeader header_, ReadBuffer & in_, Params params_)
    : IRowInputFormat(header_, in_, std::move(params_))
{
    if (header_->columns() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the number of columns is {}",
            header_->columns());

    if (!isString(removeNullable(removeLowCardinality(header_->getByPosition(0).type))))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "This input format is only suitable for tables with a single column of type String but the column type is {}",
            header_->getByPosition(0).type->getName());
}

bool RawBLOBRowInputFormat::readRow(MutableColumns & columns, RowReadExtension &)
{
    if (in->eof())
        return false;

    /// One excessive copy.
    String blob;
    readStringUntilEOF(blob, *in);
    columns.at(0)->insertData(blob.data(), blob.size());
    return false;
}

size_t RawBLOBRowInputFormat::countRows(size_t)
{
    if (done_count_rows)
        return 0;

    done_count_rows = true;
    return 1;
}

void registerInputFormatRawBLOB(FormatFactory & factory);
void registerInputFormatRawBLOB(FormatFactory & factory)
{
    factory.registerInputFormat("RawBLOB", [](
            ReadBuffer & buf,
            const Block & sample,
            const RowInputFormatParams & params,
            const FormatSettings &)
    {
        return std::make_shared<RawBLOBRowInputFormat>(std::make_shared<const Block>(sample), buf, params);
    });

    factory.setDocumentation("RawBLOB", Documentation{
        .description = R"DOCS_MD(
## Description {#description}

The `RawBLOB` formats reads all input data to a single value. It is possible to parse only a table with a single field of type [`String`](/sql-reference/data-types/string.md) or similar.
The result is output as a binary format without delimiters and escaping. If more than one value is output, the format is ambiguous, and it will be impossible to read the data back.

### Raw formats comparison {#raw-formats-comparison}

Below is a comparison of the formats `RawBLOB` and [`TabSeparatedRaw`](./TabSeparated/TabSeparatedRaw.md).

`RawBLOB`:
- data is output in binary format, no escaping;
- there are no delimiters between values;
- no new-line at the end of each value.

`TabSeparatedRaw`:
- data is output without escaping;
- the rows contain values separated by tabs;
- there is a line feed after the last value in every row.

The following is a comparison of the `RawBLOB` and [RowBinary](./RowBinary/RowBinary.md) formats.

`RawBLOB`:
- String fields are output without being prefixed by length.

`RowBinary`:
- String fields are represented as length in varint format (unsigned [LEB128] (https://en.wikipedia.org/wiki/LEB128)), followed by the bytes of the string.

When empty data is passed to the `RawBLOB` input, ClickHouse throws an exception:

```text
Code: 108. DB::Exception: No data to insert
```

## Example usage {#example-usage}

```bash title="Query"
$ clickhouse-client --query "CREATE TABLE {some_table} (a String) ENGINE = Memory;"
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT RawBLOB"
$ clickhouse-client --query "SELECT * FROM {some_table} FORMAT RawBLOB" | md5sum
```

```text title="Response"
f9725a22f9191e064120d718e26862a9  -
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerRawBLOBSchemaReader(FormatFactory & factory);
void registerRawBLOBSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("RawBLOB", [](
            const FormatSettings &)
    {
        return std::make_shared<RawBLOBSchemaReader>();
    });
}

}
