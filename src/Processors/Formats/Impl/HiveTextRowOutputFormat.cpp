#include <Processors/Formats/Impl/HiveTextRowOutputFormat.h>
#include <Formats/FormatFactory.h>

#include "config.h"

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// Reject unsupported types upfront, from the declared header rather than from the values.
/// The per-value checks in serializeTextHive are not enough: `SerializationNullable` writes `\N`
/// without descending into the nested serializer and empty `Array`/`Map` values never invoke the
/// element serializer at all, so, e.g., `CAST(NULL, 'Nullable(Time)')` or
/// `CAST([], 'Array(Decimal(39, 2))')` would silently produce a file whose declared schema no
/// Hive table could have. The walk must descend through every wrapper whose serializeTextHive is
/// a transparent pass-through (Nullable), otherwise an unsupported type hidden inside, e.g.,
/// Nullable(Tuple(Map(Array(UInt8), UInt8))) would slip past the check and still be written.
///
/// The walk also tracks the Hive separator nesting level, because Hive's LazySimpleSerDe has a
/// fixed list of 8 separators indexed by nesting depth (see getHiveTextDelimiter): a collection
/// at level N separates its elements with separator N, and a Map additionally separates keys
/// from values with separator N + 1. A type tree deep enough to need a separator beyond that
/// list could not be declared by any Hive schema either, and it has to be rejected upfront for
/// the same reason as the unsupported leaf types: an empty over-deep collection serializes
/// successfully because the deeper serializers are never reached, so without this check the
/// first non-empty value would fail only after formatting has already started. The top-level
/// columns are separated by the fields delimiter (level 0), so the outermost collection of a
/// column uses level 1, matching the FormatSettings::HiveText::nesting_level default.
void assertTypeIsSupported(const DataTypePtr & type, size_t nesting_level)
{
    /// getHiveTextDelimiter supports levels 0..7. Keep its message for consistency: this is the
    /// same error the per-value serialization throws when a non-empty value reaches this depth.
    static constexpr size_t max_nesting_level = 7;
    auto assert_nesting_level_is_supported = [](size_t level)
    {
        if (level > max_nesting_level)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The data is nested too deeply for the HiveText output format, which supports at "
                "most 8 nesting levels of separators (matching Apache Hive's LazySimpleSerDe)");
    };

    if (const auto * type_nullable = typeid_cast<const DataTypeNullable *>(type.get()))
    {
        assertTypeIsSupported(type_nullable->getNestedType(), nesting_level);
    }
    else if (const auto * type_array = typeid_cast<const DataTypeArray *>(type.get()))
    {
        assert_nesting_level_is_supported(nesting_level);
        assertTypeIsSupported(type_array->getNestedType(), nesting_level + 1);
    }
    else if (const auto * type_map = typeid_cast<const DataTypeMap *>(type.get()))
    {
        /// A Map at level N uses separator N between entries and separator N + 1 between a key
        /// and its value, so it needs one more level than an Array or a Tuple at the same depth.
        assert_nesting_level_is_supported(nesting_level + 1);
        /// Hive declares maps as MAP<primitive_type, data_type>: a map key cannot be a nested
        /// (ARRAY/MAP/STRUCT) type, so no Hive schema could read such values back. ClickHouse
        /// allows composite Map keys whose elements would serialize fine on their own, so they
        /// need an explicit rejection with a dedicated message.
        WhichDataType key_type(type_map->getKeyType());
        if (key_type.isArray() || key_type.isMap() || key_type.isTuple())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Type {} is not supported by the HiveText output format: Hive supports only primitive types as Map keys",
                type_map->getName());
        /// `Nothing` is accepted as a leaf below, because it has no values to serialize, but it is
        /// not a type any Hive schema could declare. That is harmless for the `NULL` and `[]`
        /// literals, whose types are `Nullable(Nothing)` and `Array(Nothing)` and which serialize
        /// the same way for any element type, but not for a Map key: `map()` is typed
        /// `Map(Nothing, Nothing)`, and `MAP<key_type, data_type>` needs a concrete primitive key,
        /// so such a map has to be rejected for the same reason as a composite one.
        if (key_type.isNothing())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Type {} is not supported by the HiveText output format: Hive requires a concrete primitive type as a Map key",
                type_map->getName());
        assertTypeIsSupported(type_map->getKeyType(), nesting_level + 2);
        assertTypeIsSupported(type_map->getValueType(), nesting_level + 2);
    }
    else if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        assert_nesting_level_is_supported(nesting_level);
        for (const auto & element : type_tuple->getElements())
            assertTypeIsSupported(element, nesting_level + 1);
    }
    else
    {
        switch (type->getTypeId())
        {
            /// Types with a serializeTextHive implementation. `Nothing` is allowed because it has
            /// no values to serialize: rejecting it would break, e.g., `SELECT NULL FORMAT HiveText`
            /// (whose type is `Nullable(Nothing)`) and `SELECT [] FORMAT HiveText` (`Array(Nothing)`).
            /// The one position where it cannot be allowed is a Map key, which the Map branch above
            /// rejects separately.
            case TypeIndex::Nothing:
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::UInt64:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Int64:
            case TypeIndex::BFloat16:
            case TypeIndex::Float32:
            case TypeIndex::Float64:
            case TypeIndex::Decimal32:
            case TypeIndex::Decimal64:
            case TypeIndex::Decimal128:
            case TypeIndex::String:
            case TypeIndex::Date:
            case TypeIndex::Date32:
            case TypeIndex::DateTime:
            case TypeIndex::DateTime64:
                return;

            /// Hive `DECIMAL` supports precision up to 38 only, so `Decimal256` (precisions 39..76)
            /// could not be declared by any Hive schema. Keep the message of the per-value check
            /// in `SerializationDecimal::serializeTextHive`.
            case TypeIndex::Decimal256:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Decimal precision {} is not supported by the HiveText output format: the maximum precision of Hive DECIMAL is {}",
                    getDecimalPrecision(*type), DecimalUtils::max_precision<Decimal128>);

            /// Numeric-backed types with no Hive counterpart: spell the type family the same way
            /// the corresponding per-value checks in serializeTextHive do.
            case TypeIndex::Enum8:
            case TypeIndex::Enum16:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type Enum is not supported by the HiveText output format");
            case TypeIndex::Time:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type Time is not supported by the HiveText output format");
            case TypeIndex::Time64:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type Time64 is not supported by the HiveText output format");
            case TypeIndex::Interval:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Type Interval is not supported by the HiveText output format");

            /// Everything else (the wide integers, AggregateFunction, Dynamic, Variant,
            /// LowCardinality, Object, UUID, IPv4/IPv6, FixedString, QBit, ...) has no Hive text
            /// representation either.
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Type {} is not supported by the HiveText output format", type->getName());
        }
    }
}

}


HiveTextRowOutputFormat::HiveTextRowOutputFormat(WriteBuffer & out_, SharedHeader header_, const FormatSettings & format_settings_)
    : IRowOutputFormat(header_, out_), format_settings(format_settings_)
{
    for (const auto & column : *header_)
        assertTypeIsSupported(column.type, /* nesting_level = */ 1);
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
    /// The page mirrors `docs/en/interfaces/formats/HiveText.md`; the input-related parts are
    /// included only when the input format is actually compiled in, so builds without `USE_HIVE`
    /// advertise an explicitly output-only contract.
    factory.setDocumentation("HiveText", Documentation{
        .description =
#if USE_HIVE
        R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`HiveText` reads and writes the text serialization format used by [Apache Hive](https://hive.apache.org/)
tables (the format produced by Hive's `LazySimpleSerDe`). It is a delimited text
format, similar to [`CSV`](/reference/formats/CSV/CSV), in which fields are
separated by the Hive default `\x01` (Ctrl-A) delimiter. The field delimiter is
configurable via [`input_format_hive_text_fields_delimiter`](#format-settings).

When used as an input format, the data has no header row: values are
mapped positionally onto the columns of the destination table, so the column
names and types are taken from the table (or from an explicitly provided
structure) rather than inferred from the data. While reading, ClickHouse parses
dates and times in best-effort mode (see [`date_time_input_format`](/reference/settings/formats/date-time#date_time_input_format)),
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
)DOCS_MD"
#else
        R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

`HiveText` writes the text serialization format used by [Apache Hive](https://hive.apache.org/)
tables (the format produced by Hive's `LazySimpleSerDe`). It is a delimited text
format, similar to [`CSV`](/reference/formats/CSV/CSV), in which fields are
separated by the Hive default `\x01` (Ctrl-A) delimiter. The field delimiter is
configurable via [`input_format_hive_text_fields_delimiter`](#format-settings).

<Info>
**Output-only in this build**

This ClickHouse build was compiled without Apache Hive support, so `HiveText`
is available only as an output format. Reading `HiveText` input requires a
build with Hive support enabled.
</Info>
)DOCS_MD"
#endif
        R"DOCS_MD(
## Output {#output}

When used as an output format, `HiveText` writes each row without any quoting:
top-level fields are separated by the fields delimiter (`\x01` by default) and
rows are separated by the rows delimiter (`\n` by default, configurable via
[`format_hive_text_rows_delimiter`](#format-settings)). Values of nested types
([`Array`](/reference/data-types/array), [`Map`](/reference/data-types/map)
and [`Tuple`](/reference/data-types/tuple)) are written without brackets and
are separated by the Hive separator for their nesting level, the same way Hive's
`LazySimpleSerDe` does it. The first three separators are the configurable fields
delimiter, [`input_format_hive_text_collection_items_delimiter`](#format-settings)
(`\x02` by default, used for array elements, map entries and tuple elements) and
[`input_format_hive_text_map_keys_delimiter`](#format-settings) (`\x03` by default,
used between a map key and its value); deeper levels default to consecutive control
characters (`\x04`, `\x05`, and so on, up to eight levels). A type tree nested
deeply enough to need a separator beyond those eight levels is rejected with a
`NOT_IMPLEMENTED` exception, since Hive's `LazySimpleSerDe` has no separator for
it either. Data types that have no natural
Hive text representation are not supported for output and raise a
`NOT_IMPLEMENTED` exception. This includes `AggregateFunction`, `Dynamic`,
`Variant`, `LowCardinality` and `Object`, as well as the numeric-backed types
`Enum`, `Time`, `Time64` and `Interval` — Hive has no matching type for the
latter, so they are rejected rather than written as their raw underlying
numbers. The wide numeric types `Int128`, `UInt128`, `Int256` and `UInt256`
are rejected for the same reason: the widest Hive integer is `BIGINT` (64-bit),
and even Hive `DECIMAL` with its maximum precision of 38 cannot hold their
value range. Likewise, `Decimal` values with a precision above 38 (that is,
`Decimal256`) exceed the maximum precision of Hive `DECIMAL` and are rejected.
Likewise, `Map` keys must be of a primitive type: Hive declares maps
as `MAP<primitive_type, data_type>`, so a `Map` whose key type is an `Array`,
`Map` or `Tuple` (which ClickHouse permits) is rejected with a
`NOT_IMPLEMENTED` exception, because no Hive schema could read such values
back. The empty map literal `map()` is rejected for the same reason: its type
is `Map(Nothing, Nothing)`, and `Nothing` is not a type that a Hive
`MAP<key_type, data_type>` declaration could name. All these checks are applied upfront to the declared column types, before
any row is written: a query whose header contains an unsupported type anywhere
in its type tree is rejected even when the actual values would never reach the
unsupported serialization (for example, a `Nullable` of an unsupported type
holding only `NULL` values, or an empty `Array`/`Map` of an unsupported element
type), because the file's declared schema still could not belong to any Hive
table.

`Date`, `Date32`, `DateTime` and `DateTime64` are always written in the plain
Hive date and timestamp text (`yyyy-MM-dd` and `yyyy-MM-dd HH:mm:ss[.fffffffff]`),
independent of the [`date_time_output_format`](/reference/settings/formats/date-time#date_time_output_format)
setting, so the output stays parseable by Hive even when that setting is
`unix_timestamp` or `iso`.

For the same reason, `Bool` values are always written as `true`/`false`,
independent of the [`bool_true_representation`](/reference/settings/formats/bool#bool_true_representation)
and [`bool_false_representation`](/reference/settings/formats/bool#bool_false_representation)
settings, and `NULL` values are always written as Hive's default null sequence
`\N`, independent of the [`format_csv_null_representation`](/reference/settings/formats/format-csv#format_csv_null_representation)
setting. This keeps the output readable by Hive's `LazySimpleSerDe` regardless of
these generic text settings. Symmetrically, the `HiveText` input format always
reads `\N` as `NULL`, also independent of the
[`format_csv_null_representation`](/reference/settings/formats/format-csv#format_csv_null_representation)
setting, so the top-level scalar round-trip does not depend on it.

Non-finite `Float32` and `Float64` values are written using Hive's Java spellings
`NaN`, `Infinity` and `-Infinity`, rather than ClickHouse's usual `nan`/`inf`/`-inf`
tokens, so that Hive's `FLOAT`/`DOUBLE` parser reads them back as the same values
instead of `NULL`.

<Info>
**Hive-compatible output, not a full round-trip through the input format**

The output side targets Hive's default `LazySimpleSerDe` and is not symmetric with
ClickHouse's own `HiveText` input:

- Nested [`Array`](/reference/data-types/array), [`Map`](/reference/data-types/map)
  and [`Tuple`](/reference/data-types/tuple) values are written with Hive's nested
  separators (without brackets), but the input format parses each field with
  `CSV`/bracketed rules and ignores
  [`input_format_hive_text_collection_items_delimiter`](#format-settings) /
  [`input_format_hive_text_map_keys_delimiter`](#format-settings). So nested output such
  as `SELECT [1, 2] FORMAT HiveText` is **not** read back by
  `INSERT ... FORMAT HiveText` — only top-level scalar fields round-trip, and only with
  the default `\n` row delimiter (see the next point).
- Round-tripping also requires the default `\n` row delimiter. When
  [`format_hive_text_rows_delimiter`](#format-settings) is changed, the output separates
  rows with the configured byte, but the input side is still the newline-based
  `CSVRowInputFormat` and there is no matching `input_format_hive_text_rows_delimiter`. So
  multi-row scalar output such as
  `SELECT number FROM numbers(3) FORMAT HiveText SETTINGS format_hive_text_rows_delimiter=';'`
  (which produces `0;1;2;`) is **not** read back by `INSERT ... FORMAT HiveText` as three rows.
- Only the default, unescaped `LazySimpleSerDe` subset is implemented. Fields are written
  without escaping (there is no equivalent of Hive's optional `ROW FORMAT DELIMITED ...
  ESCAPED BY`), and `NULL` is always written as `\N` (there is no equivalent of
  `NULL DEFINED AS`). A `String` that itself contains an active field, row or nested
  separator is therefore written literally and will be misread when parsed back — this
  matches how Hive itself behaves with a non-escaping serde. For the same reason a
  `String` whose value is literally `\N` (for example
  `SELECT '\\N'::String FORMAT HiveText`) is written as the same two bytes as a
  real `NULL`, so the two are indistinguishable on the Hive side.
</Info>

```sql title="Query"
SELECT '20240305', tuple(123567, 'e01001', map('action1', 33333, 'act2', 5555)) FORMAT HiveText;
```

## Format settings {#format-settings}

| Setting                                                | Description                                                                                                                           | Default |
|--------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------|---------|
| `input_format_hive_text_fields_delimiter`              | Delimiter between fields in Hive Text File                                                                                             | `\x01`  |
| `input_format_hive_text_collection_items_delimiter`    | Delimiter between collection (array or map) items in Hive Text File. Used by the output format; accepted but currently not used during input parsing.   | `\x02`  |
| `input_format_hive_text_map_keys_delimiter`            | Delimiter between a pair of map key/values in Hive Text File. Used by the output format; accepted but currently not used during input parsing.          | `\x03`  |
| `input_format_hive_text_allow_variable_number_of_columns` | Ignore extra columns in Hive Text input (if file has more columns than expected) and treat missing fields as default values        | `1`     |
| `format_hive_text_rows_delimiter`                      | Delimiter at the end of each row in Hive Text output                                                                                   | `\n`    |
)DOCS_MD"});
}

}
