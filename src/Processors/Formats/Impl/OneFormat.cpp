#include <Processors/Formats/Impl/OneFormat.h>
#include <Formats/FormatFactory.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

OneInputFormat::OneInputFormat(SharedHeader header, ReadBuffer & in_) : IInputFormat(header, &in_)
{
    if (header->columns() != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "One input format is only suitable for tables with a single column of type UInt8 but the number of columns is {}",
                        header->columns());

    if (!WhichDataType(header->getByPosition(0).type).isUInt8())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "One input format is only suitable for tables with a single column of type String but the column type is {}",
                        header->getByPosition(0).type->getName());
}

Chunk OneInputFormat::read()
{
    if (done)
        return {};

    done = true;
    auto column = ColumnUInt8::create();
    column->insertDefault();
    return Chunk(Columns{std::move(column)}, 1);
}

void registerInputFormatOne(FormatFactory & factory);
void registerInputFormatOne(FormatFactory & factory)
{
    factory.registerInputFormat("One", [](
                   ReadBuffer & buf,
                   const Block & sample,
                   const RowInputFormatParams &,
                   const FormatSettings &)
    {
        return std::make_shared<OneInputFormat>(std::make_shared<const Block>(sample), buf);
    });

    factory.setDocumentation("One", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✗      |       |

## Description {#description}

The `One` format is a special input format that doesn't read any data from file, and returns only one row with column of type [`UInt8`](../../sql-reference/data-types/int-uint.md), name `dummy` and value `0` (like the `system.one` table).
Can be used with virtual columns `_file/_path`  to list all files without reading actual data.

## Example usage {#example-usage}

Example:

```sql title="Query"
SELECT _file FROM file('path/to/files/data*', One);
```

```text title="Response"
┌─_file────┐
│ data.csv │
└──────────┘
┌─_file──────┐
│ data.jsonl │
└────────────┘
┌─_file────┐
│ data.tsv │
└──────────┘
┌─_file────────┐
│ data.parquet │
└──────────────┘
```

## Format settings {#format-settings}
)DOCS_MD"});
}

void registerOneSchemaReader(FormatFactory & factory);
void registerOneSchemaReader(FormatFactory & factory)
{
    factory.registerExternalSchemaReader("One", [](const FormatSettings &)
    {
         return std::make_shared<OneSchemaReader>();
    });
}

}
