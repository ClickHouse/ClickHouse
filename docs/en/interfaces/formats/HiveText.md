---
alias: []
description: 'Documentation for the HiveText format'
input_format: true
keywords: ['HiveText']
output_format: true
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`HiveText` reads the text serialization format used by [Apache Hive](https://hive.apache.org/)
tables (the format produced by Hive's `LazySimpleSerDe`). It is a delimited text
format, similar to [`CSV`](/interfaces/formats/CSV), in which fields are
separated by the Hive default `\x01` (Ctrl-A) delimiter. The field delimiter is
configurable via [`input_format_hive_text_fields_delimiter`](#format-settings).

`HiveText` can be used both as an input and an output format. When used as an
input format, the data has no header row: values are
mapped positionally onto the columns of the destination table, so the column
names and types are taken from the table (or from an explicitly provided
structure) rather than inferred from the data. While reading, ClickHouse parses
dates and times in best-effort mode (see [`date_time_input_format`](/operations/settings/formats#date_time_input_format)),
fills omitted trailing fields with column defaults, and skips fields it does not
recognize.

Within a field, values are parsed using the same escaping rules as `CSV` rather
than Hive's nested delimiters. In particular, a column of type
[`Array`](/sql-reference/data-types/array) is read from the bracketed
representation (for example, `"['a','b','c']"`), not from values separated by
the Hive collection delimiter `\x02`.

:::note Nested delimiter settings have no effect
The [`input_format_hive_text_collection_items_delimiter`](#format-settings) and
[`input_format_hive_text_map_keys_delimiter`](#format-settings) settings are
accepted for compatibility but are currently not used during parsing.
:::

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

## Output {#output}

When used as an output format, `HiveText` writes each row without any quoting:
top-level fields are separated by the fields delimiter (`\x01` by default) and
rows are separated by the rows delimiter (`\n` by default, configurable via
[`format_hive_text_rows_delimiter`](#format-settings)). Values of nested types
([`Array`](/sql-reference/data-types/array), [`Map`](/sql-reference/data-types/map)
and [`Tuple`](/sql-reference/data-types/tuple)) are written without brackets and
are separated by the Hive separator for their nesting level, the same way Hive's
`LazySimpleSerDe` does it. The first three separators are the configurable fields
delimiter, [`input_format_hive_text_collection_items_delimiter`](#format-settings)
(`\x02` by default, used for array elements, map entries and tuple elements) and
[`input_format_hive_text_map_keys_delimiter`](#format-settings) (`\x03` by default,
used between a map key and its value); deeper levels default to consecutive control
characters (`\x04`, `\x05`, and so on, up to eight levels). Data types that have no natural
Hive text representation are not supported for output and raise a
`NOT_IMPLEMENTED` exception. This includes `AggregateFunction`, `Dynamic`,
`Variant`, `LowCardinality` and `Object`, as well as the numeric-backed types
`Enum`, `Time`, `Time64` and `Interval` — Hive has no matching type for the
latter, so they are rejected rather than written as their raw underlying
numbers.

`Date`, `Date32`, `DateTime` and `DateTime64` are always written in the plain
Hive date and timestamp text (`yyyy-MM-dd` and `yyyy-MM-dd HH:mm:ss[.fffffffff]`),
independent of the [`date_time_output_format`](/operations/settings/formats#date_time_output_format)
setting, so the output stays parseable by Hive even when that setting is
`unix_timestamp` or `iso`.

For the same reason, `Bool` values are always written as `true`/`false`,
independent of the [`bool_true_representation`](/operations/settings/formats#bool_true_representation)
and [`bool_false_representation`](/operations/settings/formats#bool_false_representation)
settings, and `NULL` values are always written as Hive's default null sequence
`\N`, independent of the [`format_csv_null_representation`](/operations/settings/formats#format_csv_null_representation)
setting. This keeps the output readable by Hive's `LazySimpleSerDe` regardless of
these generic text settings.

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