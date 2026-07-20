#include <Processors/Formats/Impl/HiveTextRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Hive declares maps as MAP<primitive_type, data_type>: a map key cannot be a nested
/// (ARRAY/MAP/STRUCT) type, so no Hive schema could read such values back. ClickHouse allows
/// composite Map keys whose elements would serialize fine on their own, so reject them upfront.
/// The walk must descend through every wrapper whose serializeTextHive is a transparent
/// pass-through (Nullable), otherwise a composite-key Map hidden inside, e.g.,
/// Nullable(Tuple(Map(Array(UInt8), UInt8))) would slip past the check and still be written.
void assertMapKeysArePrimitive(const DataTypePtr & type)
{
    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        assertMapKeysArePrimitive(type_nullable->getNestedType());
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        assertMapKeysArePrimitive(type_array->getNestedType());
    }
    else if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        WhichDataType key_type(type_map->getKeyType());
        if (key_type.isArray() || key_type.isMap() || key_type.isTuple())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Type {} is not supported by the HiveText output format: Hive supports only primitive types as Map keys",
                type_map->getName());
        assertMapKeysArePrimitive(type_map->getValueType());
    }
    else if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        for (const auto & element : type_tuple->getElements())
            assertMapKeysArePrimitive(element);
    }
}

}


HiveTextRowOutputFormat::HiveTextRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_)
{
    for (const auto & column : *header_)
        assertMapKeysArePrimitive(column.type);
}

void HiveTextRowOutputFormat::writeField(const IColumn & column, const ISerialization & serialization, size_t row_num)
{
    serialization.serializeTextHive(column, row_num, out, format_settings);
}

void HiveTextRowOutputFormat::writeFieldDelimiter()
{
    writeChar(format_settings.hive_text.fields_delimiter, out);
}

void HiveTextRowOutputFormat::writeRowEndDelimiter()
{
    writeChar(format_settings.hive_text.rows_delimiter, out);
}

void registerOutputFormatHiveText(FormatFactory & factory);
void registerOutputFormatHiveText(FormatFactory & factory)
{
    factory.registerOutputFormat("HiveText", [](
                   WriteBuffer & buf,
                   const Block & sample,
                   const FormatSettings & format_settings,
                   FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<HiveTextRowOutputFormat>(buf, std::make_shared<const Block>(sample), format_settings);
        });
    factory.markOutputFormatSupportsParallelFormatting("HiveText");

    /// The documentation is registered here rather than next to the input format, because the
    /// output format is compiled unconditionally while the input format is gated behind `USE_HIVE`.
    /// Registering it here guarantees a non-empty description for `HiveText` in every build.
    factory.setDocumentation("HiveText", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`HiveText` reads and writes the text serialization format used by [Apache Hive](https://hive.apache.org/)
tables (the format produced by Hive's `LazySimpleSerDe`). It is a delimited text
format, similar to [`CSV`](/reference/formats/CSV/CSV), in which fields are
separated by the Hive default `\x01` (Ctrl-A) delimiter. The field delimiter is
configurable via [`input_format_hive_text_fields_delimiter`](#format-settings).

The data has no header row: values are
mapped positionally onto the columns of the destination table, so the column
names and types are taken from the table (or from an explicitly provided
structure) rather than inferred from the data. While reading, ClickHouse parses
dates and times in best-effort mode (see [`date_time_input_format`](/reference/settings/formats#date_time_input_format)),
fills omitted trailing fields with column defaults, and skips fields it does not
recognize.

Within a field, values are parsed using the same escaping rules as `CSV` rather
than Hive's nested delimiters. In particular, a column of type
[`Array`](/reference/data-types/array) is read from the bracketed
representation (for example, `"['a','b','c']"`), not from values separated by
the Hive collection delimiter `\x02`.

<Info>
**Nested delimiter settings have no effect on input**

The [`input_format_hive_text_collection_items_delimiter`](#format-settings) and
[`input_format_hive_text_map_keys_delimiter`](#format-settings) settings are
accepted for compatibility but are currently not used during parsing. They are,
however, used when writing nested values on the output side.
</Info>

By default, rows are allowed to have a variable number of fields (see
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)):
rows with fewer fields than the table have the missing columns filled with
default values, and rows with extra trailing fields have the extras skipped.

On the output side, `HiveText` writes rows using the Hive `LazySimpleSerDe`
delimiters: fields are separated by `input_format_hive_text_fields_delimiter`,
rows by [`format_hive_text_rows_delimiter`](#format-settings), and nested
`Array`/`Map`/`Tuple` values by the Hive collection-items and map-keys
delimiters. `NULL` is written as `\N`. Types that have no Hive text
representation (such as `AggregateFunction`, `Dynamic`, `Variant`,
`LowCardinality`, `Object`, `Enum`, `Time`, `Time64`, and `Interval`) are not
supported and raise an exception.

## Example usage {#example-usage}

The examples below override the default field delimiter with a comma (`,`) using
[`input_format_hive_text_fields_delimiter`](#format-settings) so that the input
files are easy to read.

### Reading a HiveText file {#reading-data}

Given a file `hive_data.txt` with comma-separated fields:

```text title="hive_data.txt"
1,3
3,5,9
```

We create a table that defines the column names and types, and insert the file
into it with `FORMAT HiveText`:

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

Note that the first row, `1,3`, has only two fields, so the missing column `c`
is filled with its default value `0`.

### Variable number of columns {#variable-number-of-columns}

With the default `input_format_hive_text_allow_variable_number_of_columns = 1`,
rows that have more fields than the table simply have the extra trailing fields
skipped:

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

Setting `input_format_hive_text_allow_variable_number_of_columns = 0` instead
enforces a strict field count, and a row with fewer fields than the table raises
a parsing exception.

## Format settings {#format-settings}

| Setting                                                | Description                                                                                                                           | Default |
|--------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------|
| `input_format_hive_text_fields_delimiter`              | Delimiter between fields in Hive Text File                                                                                             | `\x01`  |
| `input_format_hive_text_collection_items_delimiter`    | Delimiter between collection (array or map) items in Hive Text File. Accepted but not used during input parsing; used on output.       | `\x02`  |
| `input_format_hive_text_map_keys_delimiter`            | Delimiter between a pair of map key/values in Hive Text File. Accepted but not used during input parsing; used on output.              | `\x03`  |
| `input_format_hive_text_allow_variable_number_of_columns` | Ignore extra columns in Hive Text input (if file has more columns than expected) and treat missing fields as default values        | `1`     |
| `format_hive_text_rows_delimiter`                      | Delimiter between rows in the Hive Text output format                                                                                  | `\n`    |
)DOCS_MD"});
}

}
