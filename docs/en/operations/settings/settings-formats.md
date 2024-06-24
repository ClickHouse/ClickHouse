---
sidebar_label: Format Settings
sidebar_position: 52
slug: /en/operations/settings/formats
toc_max_heading_level: 2
---

# Format settings {#format-settings}

## format_display_secrets_in_show_and_select {#format_display_secrets_in_show_and_select}

Enables or disables showing secrets in `SHOW` and `SELECT` queries for tables, databases,
table functions, and dictionaries.

User wishing to see secrets must also have
[`display_secrets_in_show_and_select` server setting](../server-configuration-parameters/settings#display_secrets_in_show_and_select)
turned on and a
[`displaySecretsInShowAndSelect`](../../sql-reference/statements/grant#grant-display-secrets) privilege.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## input_format_skip_unknown_fields {#input_format_skip_unknown_fields}

Enables or disables skipping insertion of extra data.

When writing data, ClickHouse throws an exception if input data contain columns that do not exist in the target table. If skipping is enabled, ClickHouse does not insert extra data and does not throw an exception.

Supported formats:

- [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) (and other JSON formats)
- [BSONEachRow](../../interfaces/formats.md/#bsoneachrow) (and other JSON formats)
- [TSKV](../../interfaces/formats.md/#tskv)
- All formats with suffixes WithNames/WithNamesAndTypes
- [MySQLDump](../../interfaces/formats.md/#mysqldump)
- [Native](../../interfaces/formats.md/#native)

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## input_format_with_names_use_header {#input_format_with_names_use_header}

Enables or disables checking the column order when inserting data.

To improve insert performance, we recommend disabling this check if you are sure that the column order of the input data is the same as in the target table.

Supported formats:

- [CSVWithNames](../../interfaces/formats.md/#csvwithnames)
- [CSVWithNamesAndTypes](../../interfaces/formats.md/#csvwithnamesandtypes)
- [TabSeparatedWithNames](../../interfaces/formats.md/#tabseparatedwithnames)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md/#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNames](../../interfaces/formats.md/#jsoncompacteachrowwithnames)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNames](../../interfaces/formats.md/#jsoncompactstringseachrowwithnames)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNames](../../interfaces/formats.md/#rowbinarywithnames)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md/#rowbinarywithnamesandtypes)
- [CustomSeparatedWithNames](../../interfaces/formats.md/#customseparatedwithnames)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md/#customseparatedwithnamesandtypes)

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## input_format_with_types_use_header {#input_format_with_types_use_header}

Controls whether format parser should check if data types from the input data match data types from the target table.

Supported formats:

- [CSVWithNamesAndTypes](../../interfaces/formats.md/#csvwithnamesandtypes)
- [TabSeparatedWithNamesAndTypes](../../interfaces/formats.md/#tabseparatedwithnamesandtypes)
- [JSONCompactEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompacteachrowwithnamesandtypes)
- [JSONCompactStringsEachRowWithNamesAndTypes](../../interfaces/formats.md/#jsoncompactstringseachrowwithnamesandtypes)
- [RowBinaryWithNamesAndTypes](../../interfaces/formats.md/#rowbinarywithnamesandtypes-rowbinarywithnamesandtypes)
- [CustomSeparatedWithNamesAndTypes](../../interfaces/formats.md/#customseparatedwithnamesandtypes)

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## input_format_defaults_for_omitted_fields {#input_format_defaults_for_omitted_fields}

When performing `INSERT` queries, replace omitted input column values with default values of the respective columns. This option applies to [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) (and other JSON formats), [CSV](../../interfaces/formats.md/#csv), [TabSeparated](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [Parquet](../../interfaces/formats.md/#parquet), [Arrow](../../interfaces/formats.md/#arrow), [Avro](../../interfaces/formats.md/#avro), [ORC](../../interfaces/formats.md/#orc), [Native](../../interfaces/formats.md/#native) formats and formats with `WithNames`/`WithNamesAndTypes` suffixes.

:::note
When this option is enabled, extended table metadata are sent from server to client. It consumes additional computing resources on the server and can reduce performance.
:::

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## input_format_null_as_default {#input_format_null_as_default}

Enables or disables the initialization of [NULL](../../sql-reference/syntax.md/#null-literal) fields with [default values](../../sql-reference/statements/create/table.md/#create-default-values), if data type of these fields is not [nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable).
If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting.

This setting is applicable for most input formats.

For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Possible values:

- 0 — Inserting `NULL` into a not nullable column causes an exception.
- 1 — `NULL` fields are initialized with default column values.

Default value: `1`.

## input_format_allow_seeks {#input_format_allow_seeks}

Allow seeks while reading in ORC/Parquet/Arrow input formats.

Enabled by default.

## input_format_max_rows_to_read_for_schema_inference {#input_format_max_rows_to_read_for_schema_inference}

The maximum rows of data to read for automatic schema inference.

Default value: `25'000`.

## input_format_max_bytes_to_read_for_schema_inference {#input_format_max_bytes_to_read_for_schema_inference}

The maximum amount of data in bytes to read for automatic schema inference.

Default value: `33554432` (32 Mb).

## column_names_for_schema_inference {#column_names_for_schema_inference}

The list of column names to use in schema inference for formats without column names. The format: 'column1,column2,column3,...'

## schema_inference_hints {#schema_inference_hints}

The list of column names and types to use as hints in schema inference for formats without schema.

Example:

Query:
```sql
desc format(JSONEachRow, '{"x" : 1, "y" : "String", "z" : "0.0.0.0" }') settings schema_inference_hints='x UInt8, z IPv4';
```

Result:
```sql
x	UInt8
y	Nullable(String)
z	IPv4
```

:::note
If the `schema_inference_hints` is not formated properly, or if there is a typo or a wrong datatype, etc... the whole schema_inference_hints will be ignored.
:::

## schema_inference_make_columns_nullable {#schema_inference_make_columns_nullable}

Controls making inferred types `Nullable` in schema inference for formats without information about nullability.
If the setting is enabled, the inferred type will be `Nullable` only if column contains `NULL` in a sample that is parsed during schema inference.

Default value: `true`.

## input_format_try_infer_integers {#input_format_try_infer_integers}

If enabled, ClickHouse will try to infer integers instead of floats in schema inference for text formats. If all numbers in the column from input data are integers, the result type will be `Int64`, if at least one number is float, the result type will be `Float64`.

Enabled by default.

## input_format_try_infer_dates {#input_format_try_infer_dates}

If enabled, ClickHouse will try to infer type `Date` from string fields in schema inference for text formats. If all fields from a column in input data were successfully parsed as dates, the result type will be `Date`, if at least one field was not parsed as date, the result type will be `String`.

Enabled by default.

## input_format_try_infer_datetimes {#input_format_try_infer_datetimes}

If enabled, ClickHouse will try to infer type `DateTime64` from string fields in schema inference for text formats. If all fields from a column in input data were successfully parsed as datetimes, the result type will be `DateTime64`, if at least one field was not parsed as datetime, the result type will be `String`.

Enabled by default.

## date_time_input_format {#date_time_input_format}

Allows choosing a parser of the text representation of date and time.

The setting does not apply to [date and time functions](../../sql-reference/functions/date-time-functions.md).

Possible values:

- `'best_effort'` — Enables extended parsing.

    ClickHouse can parse the basic `YYYY-MM-DD HH:MM:SS` format and all [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date and time formats. For example, `'2018-06-08T01:02:03.000Z'`.

- `'basic'` — Use basic parser.

    ClickHouse can parse only the basic `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD` format. For example, `2019-08-20 10:18:56` or `2019-08-20`.

Default value: `'basic'`.

Cloud default value: `'best_effort'`.

See also:

- [DateTime data type.](../../sql-reference/data-types/datetime.md)
- [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)

## date_time_output_format {#date_time_output_format}

Allows choosing different output formats of the text representation of date and time.

Possible values:

- `simple` - Simple output format.

    ClickHouse output date and time `YYYY-MM-DD hh:mm:ss` format. For example, `2019-08-20 10:18:56`. The calculation is performed according to the data type's time zone (if present) or server time zone.

- `iso` - ISO output format.

    ClickHouse output date and time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) `YYYY-MM-DDThh:mm:ssZ` format. For example, `2019-08-20T10:18:56Z`. Note that output is in UTC (`Z` means UTC).

- `unix_timestamp` - Unix timestamp output format.

    ClickHouse output date and time in [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) format. For example `1566285536`.

Default value: `simple`.

See also:

- [DateTime data type.](../../sql-reference/data-types/datetime.md)
- [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)

## interval_output_format {#interval_output_format}

Allows choosing different output formats of the text representation of interval types.

Possible values:

-   `kusto` - KQL-style output format.

    ClickHouse outputs intervals in [KQL format](https://learn.microsoft.com/en-us/dotnet/standard/base-types/standard-timespan-format-strings#the-constant-c-format-specifier). For example, `toIntervalDay(2)` would be formatted as `2.00:00:00`. Please note that for interval types of varying length (ie. `IntervalMonth` and `IntervalYear`) the average number of seconds per interval is taken into account.

-   `numeric` - Numeric output format.

    ClickHouse outputs intervals as their underlying numeric representation. For example, `toIntervalDay(2)` would be formatted as `2`.

Default value: `numeric`.

See also:

-   [Interval](../../sql-reference/data-types/special-data-types/interval.md)

## input_format_ipv4_default_on_conversion_error {#input_format_ipv4_default_on_conversion_error}

Deserialization of IPv4 will use default values instead of throwing exception on conversion error.

Disabled by default.

## input_format_ipv6_default_on_conversion_error {#input_format_ipv6_default_on_conversion_error}

Deserialization of IPV6 will use default values instead of throwing exception on conversion error.

Disabled by default.

## bool_true_representation {#bool_true_representation}

Text to represent true bool value in TSV/CSV/Vertical/Pretty formats.

Default value: `true`

## bool_false_representation {#bool_false_representation}

Text to represent false bool value in TSV/CSV/Vertical/Pretty formats.

Default value: `false`

## output_format_decimal_trailing_zeros {#output_format_decimal_trailing_zeros}

Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.

Disabled by default.

## input_format_allow_errors_num {#input_format_allow_errors_num}

Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## input_format_allow_errors_ratio {#input_format_allow_errors_ratio}

Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## format_schema {#format-schema}

This parameter is useful when you are using formats that require a schema definition, such as [Cap’n Proto](https://capnproto.org/) or [Protobuf](https://developers.google.com/protocol-buffers/). The value depends on the format.

## output_format_schema {#output-format-schema}

The path to the file where the automatically generated schema will be saved in [Cap’n Proto](../../interfaces/formats.md#capnproto-capnproto) or [Protobuf](../../interfaces/formats.md#protobuf-protobuf) formats.

## output_format_enable_streaming {#output_format_enable_streaming}

Enable streaming in output formats that support it.

Disabled by default.

## output_format_write_statistics {#output_format_write_statistics}

Write statistics about read rows, bytes, time elapsed in suitable output formats.

Enabled by default

## insert_distributed_one_random_shard {#insert_distributed_one_random_shard}

Enables or disables random shard insertion into a [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table when there is no distributed key.

By default, when inserting data into a `Distributed` table with more than one shard, the ClickHouse server will reject any insertion request if there is no distributed key. When `insert_distributed_one_random_shard = 1`, insertions are allowed and data is forwarded randomly among all shards.

Possible values:

- 0 — Insertion is rejected if there are multiple shards and no distributed key is given.
- 1 — Insertion is done randomly among all available shards when no distributed key is given.

Default value: `0`.

## JSON formats settings {#json-formats-settings}

## input_format_import_nested_json {#input_format_import_nested_json}

Enables or disables the insertion of JSON data with nested objects.

Supported formats:

- [JSONEachRow](../../interfaces/formats.md/#jsoneachrow)

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

See also:

- [Usage of Nested Structures](../../interfaces/formats.md/#jsoneachrow-nested) with the `JSONEachRow` format.

## input_format_json_read_bools_as_numbers {#input_format_json_read_bools_as_numbers}

Allow parsing bools as numbers in JSON input formats.

Enabled by default.

## input_format_json_read_bools_as_strings {#input_format_json_read_bools_as_strings}

Allow parsing bools as strings in JSON input formats.

Enabled by default.

## input_format_json_read_numbers_as_strings {#input_format_json_read_numbers_as_strings}

Allow parsing numbers as strings in JSON input formats.

Enabled by default.

## input_format_json_try_infer_numbers_from_strings {#input_format_json_try_infer_numbers_from_strings}

If enabled, during schema inference ClickHouse will try to infer numbers from string fields.
It can be useful if JSON data contains quoted UInt64 numbers.

Disabled by default.

## input_format_json_read_objects_as_strings {#input_format_json_read_objects_as_strings}

Allow parsing JSON objects as strings in JSON input formats.

Example:

```sql
SET input_format_json_read_objects_as_strings = 1;
CREATE TABLE test (id UInt64, obj String, date Date) ENGINE=Memory();
INSERT INTO test FORMAT JSONEachRow {"id" : 1, "obj" : {"a" : 1, "b" : "Hello"}, "date" : "2020-01-01"};
SELECT * FROM test;
```

Result:

```
┌─id─┬─obj──────────────────────┬───────date─┐
│  1 │ {"a" : 1, "b" : "Hello"} │ 2020-01-01 │
└────┴──────────────────────────┴────────────┘
```

Enabled by default.

## input_format_json_try_infer_named_tuples_from_objects {#input_format_json_try_infer_named_tuples_from_objects}

If enabled, during schema inference ClickHouse will try to infer named Tuple from JSON objects.
The resulting named Tuple will contain all elements from all corresponding JSON objects from sample data.

Example:

```sql
SET input_format_json_try_infer_named_tuples_from_objects = 1;
DESC format(JSONEachRow, '{"obj" : {"a" : 42, "b" : "Hello"}}, {"obj" : {"a" : 43, "c" : [1, 2, 3]}}, {"obj" : {"d" : {"e" : 42}}}')
```

Result:

```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Nullable(Int64), b Nullable(String), c Array(Nullable(Int64)), d Tuple(e Nullable(Int64))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

Enabled by default.

## input_format_json_read_arrays_as_strings {#input_format_json_read_arrays_as_strings}

Allow parsing JSON arrays as strings in JSON input formats.

Example:

```sql
SET input_format_json_read_arrays_as_strings = 1;
SELECT arr, toTypeName(arr), JSONExtractArrayRaw(arr)[3] from format(JSONEachRow, 'arr String', '{"arr" : [1, "Hello", [1,2,3]]}');
```

Result:
```
┌─arr───────────────────┬─toTypeName(arr)─┬─arrayElement(JSONExtractArrayRaw(arr), 3)─┐
│ [1, "Hello", [1,2,3]] │ String          │ [1,2,3]                                   │
└───────────────────────┴─────────────────┴───────────────────────────────────────────┘
```

Enabled by default.

## input_format_json_infer_incomplete_types_as_strings {#input_format_json_infer_incomplete_types_as_strings}

Allow to use String type for JSON keys that contain only `Null`/`{}`/`[]` in data sample during schema inference.
In JSON formats any value can be read as String, and we can avoid errors like `Cannot determine type for column 'column_name' by first 25000 rows of data, most likely this column contains only Nulls or empty Arrays/Maps` during schema inference
by using String type for keys with unknown types.

Example:

```sql
SET input_format_json_infer_incomplete_types_as_strings = 1, input_format_json_try_infer_named_tuples_from_objects = 1;
DESCRIBE format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
SELECT * FROM format(JSONEachRow, '{"obj" : {"a" : [1,2,3], "b" : "hello", "c" : null, "d" : {}, "e" : []}}');
```

Result:
```
┌─name─┬─type───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ obj  │ Tuple(a Array(Nullable(Int64)), b Nullable(String), c Nullable(String), d Nullable(String), e Array(Nullable(String))) │              │                    │         │                  │                │
└──────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘

┌─obj────────────────────────────┐
│ ([1,2,3],'hello',NULL,'{}',[]) │
└────────────────────────────────┘
```

Enabled by default.

## input_format_json_validate_types_from_metadata {#input_format_json_validate_types_from_metadata}

For JSON/JSONCompact/JSONColumnsWithMetadata input formats, if this setting is set to 1,
the types from metadata in input data will be compared with the types of the corresponding columns from the table.

Enabled by default.

## output_format_json_quote_64bit_integers {#output_format_json_quote_64bit_integers}

Controls quoting of 64-bit or bigger [integers](../../sql-reference/data-types/int-uint.md) (like `UInt64` or `Int128`) when they are output in a [JSON](../../interfaces/formats.md/#json) format.
Such integers are enclosed in quotes by default. This behavior is compatible with most JavaScript implementations.

Possible values:

- 0 — Integers are output without quotes.
- 1 — Integers are enclosed in quotes.

Default value: 1.

## output_format_json_quote_64bit_floats {#output_format_json_quote_64bit_floats}

Controls quoting of 64-bit [floats](../../sql-reference/data-types/float.md) when they are output in JSON* formats.

Disabled by default.

## output_format_json_quote_denormals {#output_format_json_quote_denormals}

Enables `+nan`, `-nan`, `+inf`, `-inf` outputs in [JSON](../../interfaces/formats.md/#json) output format.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

**Example**

Consider the following table `account_orders`:

```text
┌─id─┬─name───┬─duration─┬─period─┬─area─┐
│  1 │ Andrew │       20 │      0 │  400 │
│  2 │ John   │       40 │      0 │    0 │
│  3 │ Bob    │       15 │      0 │ -100 │
└────┴────────┴──────────┴────────┴──────┘
```

When `output_format_json_quote_denormals = 0`, the query returns `null` values in output:

```sql
SELECT area/period FROM account_orders FORMAT JSON;
```

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.003648093,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

When `output_format_json_quote_denormals = 1`, the query returns:

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": "inf"
                },
                {
                        "divide(area, period)": "-nan"
                },
                {
                        "divide(area, period)": "-inf"
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.000070241,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## output_format_json_quote_decimals {#output_format_json_quote_decimals}

Controls quoting of decimals in JSON output formats.

Disabled by default.

## output_format_json_escape_forward_slashes {#output_format_json_escape_forward_slashes}

Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don't confuse with backslashes that are always escaped.

Enabled by default.

## output_format_json_named_tuples_as_objects {#output_format_json_named_tuples_as_objects}

Serialize named tuple columns as JSON objects.

Enabled by default.

## input_format_json_named_tuples_as_objects {#input_format_json_named_tuples_as_objects}

Parse named tuple columns as JSON objects.

Enabled by default.

## input_format_json_ignore_unknown_keys_in_named_tuple {#input_format_json_ignore_unknown_keys_in_named_tuple}

Ignore unknown keys in json object for named tuples.

Enabled by default.

## input_format_json_defaults_for_missing_elements_in_named_tuple {#input_format_json_defaults_for_missing_elements_in_named_tuple}

Insert default values for missing elements in JSON object while parsing named tuple.
This setting works only when setting `input_format_json_named_tuples_as_objects` is enabled.

Enabled by default.

## input_format_json_throw_on_bad_escape_sequence {#input_format_json_throw_on_bad_escape_sequence}

Throw an exception if JSON string contains bad escape sequence in JSON input formats. If disabled, bad escape sequences will remain as is in the data.

Enabled by default.

## output_format_json_array_of_rows {#output_format_json_array_of_rows}

Enables the ability to output all rows as a JSON array in the [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) format.

Possible values:

- 1 — ClickHouse outputs all rows as an array, each row in the `JSONEachRow` format.
- 0 — ClickHouse outputs each row separately in the `JSONEachRow` format.

Default value: `0`.

**Example of a query with the enabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 1;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
[
{"number":"0"},
{"number":"1"},
{"number":"2"}
]
```

**Example of a query with the disabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 0;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
{"number":"0"}
{"number":"1"}
{"number":"2"}
```

## output_format_json_validate_utf8 {#output_format_json_validate_utf8}

Controls validation of UTF-8 sequences in JSON output formats, doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate UTF-8.

Disabled by default.

## format_json_object_each_row_column_for_object_name {#format_json_object_each_row_column_for_object_name}

The name of column that will be used for storing/writing object names in [JSONObjectEachRow](../../interfaces/formats.md/#jsonobjecteachrow) format.
Column type should be String. If value is empty, default names `row_{i}`will be used for object names.

Default value: ''.

### input_format_json_compact_allow_variable_number_of_columns {#input_format_json_compact_allow_variable_number_of_columns}

Allow variable number of columns in rows in JSONCompact/JSONCompactEachRow input formats.
Ignore extra columns in rows with more columns than expected and treat missing columns as default values.

Disabled by default.

### output_format_markdown_escape_special_characters {#output_format_markdown_escape_special_characters}

When enabled, escape special characters in Markdown.

[Common Mark](https://spec.commonmark.org/0.30/#example-12) defines the following special characters that can be escaped by \:

```
! " # $ % & ' ( ) * + , - . / : ; < = > ? @ [ \ ] ^ _ ` { | } ~
```

Possible values:

+ 0 — Disable.
+ 1 — Enable.

Default value: 0.

## TSV format settings {#tsv-format-settings}

### input_format_tsv_empty_as_default {#input_format_tsv_empty_as_default}

When enabled, replace empty input fields in TSV with default values. For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Disabled by default.

### input_format_tsv_enum_as_number {#input_format_tsv_enum_as_number}

When enabled, always treat enum values as enum ids for TSV input format. It's recommended to enable this setting if data contains only enum ids to optimize enum parsing.

Possible values:

- 0 — Enum values are parsed as values or as enum IDs.
- 1 — Enum values are parsed only as enum IDs.

Default value: 0.

**Example**

Consider the table:

```sql
CREATE TABLE table_with_enum_column_for_tsv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

When the `input_format_tsv_enum_as_number` setting is enabled:

Query:

```sql
SET input_format_tsv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
SELECT * FROM table_with_enum_column_for_tsv_insert;
```

Result:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
```

Query:

```sql
SET input_format_tsv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 103	'first';
```

throws an exception.

When the `input_format_tsv_enum_as_number` setting is disabled:

Query:

```sql
SET input_format_tsv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 103	'first';
SELECT * FROM table_with_enum_column_for_tsv_insert;
```

Result:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
┌──Id─┬─Value──┐
│ 103 │ first  │
└─────┴────────┘
```

### input_format_tsv_use_best_effort_in_schema_inference {#input_format_tsv_use_best_effort_in_schema_inference}

Use some tweaks and heuristics to infer schema in TSV format. If disabled, all fields will be treated as String.

Enabled by default.

### input_format_tsv_skip_first_lines {#input_format_tsv_skip_first_lines}

The number of lines to skip at the beginning of data in TSV input format.

Default value: `0`.

### output_format_tsv_crlf_end_of_line {#output_format_tsv_crlf_end_of_line}

Use DOC/Windows-style line separator (CRLF) in TSV instead of Unix style (LF).

Disabled by default.

### format_tsv_null_representation {#format_tsv_null_representation}

Defines the representation of `NULL` for [TSV](../../interfaces/formats.md/#tabseparated) output and input formats. User can set any string as a value, for example, `My NULL`.

Default value: `\N`.

**Examples**

Query

```sql
SELECT * FROM tsv_custom_null FORMAT TSV;
```

Result

```text
788
\N
\N
```

Query

```sql
SET format_tsv_null_representation = 'My NULL';
SELECT * FROM tsv_custom_null FORMAT TSV;
```

Result

```text
788
My NULL
My NULL
```

### input_format_tsv_skip_trailing_empty_lines {input_format_tsv_skip_trailing_empty_lines}

When enabled, trailing empty lines at the end of TSV file will be skipped.

Disabled by default.

### input_format_tsv_allow_variable_number_of_columns {#input_format_tsv_allow_variable_number_of_columns}

Allow variable number of columns in rows in TSV input format.
Ignore extra columns in rows with more columns than expected and treat missing columns as default values.

Disabled by default.

## CSV format settings {#csv-format-settings}

### format_csv_delimiter {#format_csv_delimiter}

The character is interpreted as a delimiter in the CSV data.

Default value: `,`.

### format_csv_allow_single_quotes {#format_csv_allow_single_quotes}

If it is set to true, allow strings in single quotes.

Disabled by default.

### format_csv_allow_double_quotes {#format_csv_allow_double_quotes}

If it is set to true, allow strings in double quotes.

Enabled by default.

### output_format_csv_crlf_end_of_line {#output_format_csv_crlf_end_of_line}

Use DOS/Windows-style line separator (CRLF) in CSV instead of Unix style (LF).

Disabled by default.

### input_format_csv_allow_cr_end_of_line {#input_format_csv_allow_cr_end_of_line}

If it is set true, CR(\\r) will be allowed at end of line not followed by LF(\\n)

Disabled by default.

### input_format_csv_enum_as_number {#input_format_csv_enum_as_number}

When enabled, always treat enum values as enum ids for CSV input format. It's recommended to enable this setting if data contains only enum ids to optimize enum parsing.

Possible values:

- 0 — Enum values are parsed as values or as enum IDs.
- 1 — Enum values are parsed only as enum IDs.

Default value: 0.

**Examples**

Consider the table:

```sql
CREATE TABLE table_with_enum_column_for_csv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

When the `input_format_csv_enum_as_number` setting is enabled:

Query:

```sql
SET input_format_csv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2
```

Result:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
```

Query:

```sql
SET input_format_csv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 103,'first'
```

throws an exception.

When the `input_format_csv_enum_as_number` setting is disabled:

Query:

```sql
SET input_format_csv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 103,'first'
SELECT * FROM table_with_enum_column_for_csv_insert;
```

Result:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
┌──Id─┬─Value─┐
│ 103 │ first │
└─────┴───────┘
```

### input_format_csv_arrays_as_nested_csv {#input_format_csv_arrays_as_nested_csv}

When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: "[""Hello"", ""world"", ""42"""" TV""]". Braces around array can be omitted.

Disabled by default.

### input_format_csv_empty_as_default {#input_format_csv_empty_as_default}

When enabled, replace empty input fields in CSV with default values. For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Enabled by default.

### input_format_csv_use_best_effort_in_schema_inference {#input_format_csv_use_best_effort_in_schema_inference}

Use some tweaks and heuristics to infer schema in CSV format. If disabled, all fields will be treated as String.

Enabled by default.

### input_format_csv_skip_first_lines {#input_format_csv_skip_first_lines}

The number of lines to skip at the beginning of data in CSV input format.

Default value: `0`.

### format_csv_null_representation {#format_csv_null_representation}

Defines the representation of `NULL` for [CSV](../../interfaces/formats.md/#csv) output and input formats. User can set any string as a value, for example, `My NULL`.

Default value: `\N`.

**Examples**

Query

```sql
SELECT * from csv_custom_null FORMAT CSV;
```

Result

```text
788
\N
\N
```

Query

```sql
SET format_csv_null_representation = 'My NULL';
SELECT * FROM csv_custom_null FORMAT CSV;
```

Result

```text
788
My NULL
My NULL
```

### input_format_csv_skip_trailing_empty_lines {input_format_csv_skip_trailing_empty_lines}

When enabled, trailing empty lines at the end of CSV file will be skipped.

Disabled by default.

### input_format_csv_trim_whitespaces {#input_format_csv_trim_whitespaces}

Trims spaces and tabs in non-quoted CSV strings.

Default value: `true`.

**Examples**

Query

```bash
echo '  string  ' | ./clickhouse local -q  "select * from table FORMAT CSV" --input-format="CSV" --input_format_csv_trim_whitespaces=true
```

Result

```text
"string"
```

Query

```bash
echo '  string  ' | ./clickhouse local -q  "select * from table FORMAT CSV" --input-format="CSV" --input_format_csv_trim_whitespaces=false
```

Result

```text
"  string  "
```

### input_format_csv_allow_variable_number_of_columns {#input_format_csv_allow_variable_number_of_columns}

Allow variable number of columns in rows in CSV input format.
Ignore extra columns in rows with more columns than expected and treat missing columns as default values.

Disabled by default.

### input_format_csv_allow_whitespace_or_tab_as_delimiter {#input_format_csv_allow_whitespace_or_tab_as_delimiter}

Allow to use whitespace or tab as field delimiter in CSV strings.

Default value: `false`.

**Examples**

Query

```bash
echo 'a b' | ./clickhouse local -q  "select * from table FORMAT CSV" --input-format="CSV" --input_format_csv_allow_whitespace_or_tab_as_delimiter=true --format_csv_delimiter=' '
```

Result

```text
a  b
```

Query

```bash
echo 'a         b' | ./clickhouse local -q  "select * from table FORMAT CSV" --input-format="CSV" --input_format_csv_allow_whitespace_or_tab_as_delimiter=true --format_csv_delimiter='\t'
```

Result

```text
a  b
```

### input_format_csv_use_default_on_bad_values {#input_format_csv_use_default_on_bad_values}

Allow to set default value to column when CSV field deserialization failed on bad value

Default value: `false`.

**Examples**

Query

```bash
./clickhouse local -q "create table test_tbl (x String, y UInt32, z Date) engine=MergeTree order by x"
echo 'a,b,c' | ./clickhouse local -q  "INSERT INTO test_tbl SETTINGS input_format_csv_use_default_on_bad_values=true FORMAT CSV"
./clickhouse local -q "select * from test_tbl"
```

Result

```text
a  0  1971-01-01
```

## input_format_csv_try_infer_numbers_from_strings {#input_format_csv_try_infer_numbers_from_strings}

If enabled, during schema inference ClickHouse will try to infer numbers from string fields.
It can be useful if CSV data contains quoted UInt64 numbers.

Disabled by default.

## Values format settings {#values-format-settings}

### input_format_values_interpret_expressions {#input_format_values_interpret_expressions}

Enables or disables the full SQL parser if the fast stream parser can’t parse the data. This setting is used only for the [Values](../../interfaces/formats.md/#data-format-values) format at the data insertion. For more information about syntax parsing, see the [Syntax](../../sql-reference/syntax.md) section.

Possible values:

- 0 — Disabled.

    In this case, you must provide formatted data. See the [Formats](../../interfaces/formats.md) section.

- 1 — Enabled.

    In this case, you can use an SQL expression as a value, but data insertion is much slower this way. If you insert only formatted data, then ClickHouse behaves as if the setting value is 0.

Default value: 1.

Example of Use

Insert the [DateTime](../../sql-reference/data-types/datetime.md) type value with the different settings.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

The last query is equivalent to the following:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

### input_format_values_deduce_templates_of_expressions {#input_format_values_deduce_templates_of_expressions}

Enables or disables template deduction for SQL expressions in [Values](../../interfaces/formats.md/#data-format-values) format. It allows parsing and interpreting expressions in `Values` much faster if expressions in consecutive rows have the same structure. ClickHouse tries to deduce the template of an expression, parse the following rows using this template and evaluate the expression on a batch of successfully parsed rows.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

For the following query:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

- If `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=0`, expressions are interpreted separately for each row (this is very slow for large number of rows).
- If `input_format_values_interpret_expressions=0` and `format_values_deduce_templates_of_expressions=1`, expressions in the first, second and third rows are parsed using template `lower(String)` and interpreted together, expression in the forth row is parsed with another template (`upper(String)`).
- If `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=1`, the same as in previous case, but also allows fallback to interpreting expressions separately if it’s not possible to deduce template.

### input_format_values_accurate_types_of_literals {#input_format_values_accurate_types_of_literals}

This setting is used only when `input_format_values_deduce_templates_of_expressions = 1`. Expressions for some column may have the same structure, but contain numeric literals of different types, e.g.

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Possible values:

- 0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` or `Int64` instead of `UInt64` for `42`), but it may cause overflow and precision issues.

- 1 — Enabled.

    In this case, ClickHouse checks the actual type of literal and uses an expression template of the corresponding type. In some cases, it may significantly slow down expression evaluation in `Values`.

Default value: 1.

## Arrow format settings {#arrow-format-settings}

### input_format_arrow_case_insensitive_column_matching {#input_format_arrow_case_insensitive_column_matching}

Ignore case when matching Arrow column names with ClickHouse column names.

Disabled by default.

### input_format_arrow_allow_missing_columns {#input_format_arrow_allow_missing_columns}

While importing data, when column is not found in schema default value will be used instead of error.

Disabled by default.

### input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference {#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference}

Allow skipping columns with unsupported types while schema inference for format Arrow.

Disabled by default.

### output_format_arrow_low_cardinality_as_dictionary {#output_format_arrow_low_cardinality_as_dictionary}

Allows to convert the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) type to the `DICTIONARY` type of the [Arrow](../../interfaces/formats.md/#data-format-arrow) format for `SELECT` queries.

Possible values:

- 0 — The `LowCardinality` type is not converted to the `DICTIONARY` type.
- 1 — The `LowCardinality` type is converted to the `DICTIONARY` type.

Default value: `0`.

### output_format_arrow_use_signed_indexes_for_dictionary {#output_format_arrow_use_signed_indexes_for_dictionary}

Use signed integer types instead of unsigned in `DICTIONARY` type of the [Arrow](../../interfaces/formats.md/#data-format-arrow) format during  [LowCardinality](../../sql-reference/data-types/lowcardinality.md) output when `output_format_arrow_low_cardinality_as_dictionary` is enabled.

Possible values:

- 0 — Unsigned integer types are used for indexes in `DICTIONARY` type.
- 1 — Signed integer types are used for indexes in `DICTIONARY` type.

Default value: `1`.

### output_format_arrow_use_64_bit_indexes_for_dictionary {#output_format_arrow_use_64_bit_indexes_for_dictionary}

Use 64-bit integer type in `DICTIONARY` type of the [Arrow](../../interfaces/formats.md/#data-format-arrow) format during  [LowCardinality](../../sql-reference/data-types/lowcardinality.md) output when `output_format_arrow_low_cardinality_as_dictionary` is enabled.

Possible values:

- 0 — Type for indexes in `DICTIONARY` type is determined automatically.
- 1 — 64-bit integer type is used for indexes in `DICTIONARY` type.

Default value: `0`.

### output_format_arrow_string_as_string {#output_format_arrow_string_as_string}

Use Arrow String type instead of Binary for String columns.

Disabled by default.

### output_format_arrow_fixed_string_as_fixed_byte_array (#output_format_arrow_fixed_string_as_fixed_byte_array)

Use Arrow FIXED_SIZE_BINARY type instead of Binary/String for FixedString columns.

Enabled by default.

### output_format_arrow_compression_method {#output_format_arrow_compression_method}

Compression method used in output Arrow format. Supported codecs: `lz4_frame`, `zstd`, `none` (uncompressed)

Default value: `lz4_frame`.

## ORC format settings {#orc-format-settings}

### input_format_orc_row_batch_size {#input_format_orc_row_batch_size}

Batch size when reading ORC stripes.

Default value: `100'000`

### input_format_orc_case_insensitive_column_matching {#input_format_orc_case_insensitive_column_matching}

Ignore case when matching ORC column names with ClickHouse column names.

Disabled by default.

### input_format_orc_allow_missing_columns {#input_format_orc_allow_missing_columns}

While importing data, when column is not found in schema default value will be used instead of error.

Disabled by default.

### input_format_orc_skip_columns_with_unsupported_types_in_schema_inference {#input_format_orc_skip_columns_with_unsupported_types_in_schema_inference}

Allow skipping columns with unsupported types while schema inference for format Arrow.

Disabled by default.

### output_format_orc_string_as_string {#output_format_orc_string_as_string}

Use ORC String type instead of Binary for String columns.

Disabled by default.

### output_format_orc_compression_method {#output_format_orc_compression_method}

Compression method used in output ORC format. Supported codecs: `lz4`, `snappy`, `zlib`, `zstd`, `none` (uncompressed)

Default value: `none`.

## Parquet format settings {#parquet-format-settings}

### input_format_parquet_case_insensitive_column_matching {#input_format_parquet_case_insensitive_column_matching}

Ignore case when matching Parquet column names with ClickHouse column names.

Disabled by default.

### output_format_parquet_row_group_size {#output_format_parquet_row_group_size}

Row group size in rows.

Default value: `1'000'000`.

### input_format_parquet_allow_missing_columns {#input_format_parquet_allow_missing_columns}

While importing data, when column is not found in schema default value will be used instead of error.

Enabled by default.

### input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference {#input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference}

Allow skipping columns with unsupported types while schema inference for format Parquet.

Disabled by default.

### input_format_parquet_local_file_min_bytes_for_seek {#input_format_parquet_local_file_min_bytes_for_seek}

min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format.

Default value - `8192`.

### output_format_parquet_string_as_string {#output_format_parquet_string_as_string}

Use Parquet String type instead of Binary for String columns.

Disabled by default.

### output_format_parquet_fixed_string_as_fixed_byte_array (#output_format_parquet_fixed_string_as_fixed_byte_array)

Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary/String for FixedString columns.

Enabled by default.

### output_format_parquet_version {#output_format_parquet_version}

The version of Parquet format used in output format. Supported versions: `1.0`, `2.4`, `2.6` and `2.latest`.

Default value: `2.latest`.

### output_format_parquet_compression_method {#output_format_parquet_compression_method}

Compression method used in output Parquet format. Supported codecs: `snappy`, `lz4`, `brotli`, `zstd`, `gzip`, `none` (uncompressed)

Default value: `lz4`.

## Hive format settings {#hive-format-settings}

### input_format_hive_text_fields_delimiter {#input_format_hive_text_fields_delimiter}

Delimiter between fields in Hive Text File.

Default value: `\x01`.

### input_format_hive_text_collection_items_delimiter {#input_format_hive_text_collection_items_delimiter}

Delimiter between collection(array or map) items in Hive Text File.

Default value: `\x02`.

### input_format_hive_text_map_keys_delimiter {#input_format_hive_text_map_keys_delimiter}

Delimiter between a pair of map key/values in Hive Text File.

Default value: `\x03`.

## MsgPack format settings {#msgpack-format-settings}

### input_format_msgpack_number_of_columns {#input_format_msgpack_number_of_columns}

The number of columns in inserted MsgPack data. Used for automatic schema inference from data.

Default value: `0`.

### output_format_msgpack_uuid_representation {#output_format_msgpack_uuid_representation}

The way how to output UUID in MsgPack format.
Possible values:

- `bin` - as 16-bytes binary.
- `str` - as a string of 36 bytes.
- `ext` - as extension with ExtType = 2.

Default value: `ext`.


## Protobuf format settings {#protobuf-format-settings}

### input_format_protobuf_flatten_google_wrappers {#input_format_protobuf_flatten_google_wrappers}

Enable Google wrappers for regular non-nested columns, e.g. google.protobuf.StringValue 'str' for String column 'str'. For Nullable columns empty wrappers are recognized as defaults, and missing as nulls.

Disabled by default.

### output_format_protobuf_nullables_with_google_wrappers {#output_format_protobuf_nullables_with_google_wrappers}

When serializing Nullable columns with Google wrappers, serialize default values as empty wrappers. If turned off, default and null values are not serialized.

Disabled by default.

### format_protobuf_use_autogenerated_schema {#format_capn_proto_use_autogenerated_schema}

Use autogenerated Protobuf schema when [format_schema](#formatschema-format-schema) is not set.
The schema is generated from ClickHouse table structure using function [structureToProtobufSchema](../../sql-reference/functions/other-functions.md#structure_to_protobuf_schema)

## Avro format settings {#avro-format-settings}

### input_format_avro_allow_missing_fields {#input_format_avro_allow_missing_fields}

Enables using fields that are not specified in [Avro](../../interfaces/formats.md/#data-format-avro) or [AvroConfluent](../../interfaces/formats.md/#data-format-avro-confluent) format schema. When a field is not found in the schema, ClickHouse uses the default value instead of throwing an exception.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

### format_avro_schema_registry_url {#format_avro_schema_registry_url}

Sets [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) URL to use with [AvroConfluent](../../interfaces/formats.md/#data-format-avro-confluent) format.

Format:
``` text
http://[user:password@]machine[:port]"
```

Examples:
``` text
http://registry.example.com:8081
http://admin:secret@registry.example.com:8081
```

Default value: `Empty`.

### output_format_avro_codec {#output_format_avro_codec}

Sets the compression codec used for output Avro file.

Type: string

Possible values:

- `null` — No compression
- `deflate` — Compress with Deflate (zlib)
- `snappy` — Compress with [Snappy](https://google.github.io/snappy/)

Default value: `snappy` (if available) or `deflate`.

### output_format_avro_sync_interval {#output_format_avro_sync_interval}

Sets minimum data size (in bytes) between synchronization markers for output Avro file.

Type: unsigned int

Possible values: 32 (32 bytes) - 1073741824 (1 GiB)

Default value: 32768 (32 KiB)

### output_format_avro_string_column_pattern {#output_format_avro_string_column_pattern}

Regexp of column names of type String to output as Avro `string` (default is `bytes`).
RE2 syntax is supported.

Type: string

### output_format_avro_rows_in_file {#output_format_avro_rows_in_file}

Max rows in a file (if permitted by storage).

Default value: `1`.

## Pretty formats settings {#pretty-formats-settings}

### output_format_pretty_max_rows {#output_format_pretty_max_rows}

Rows limit for Pretty formats.

Default value: `10'000`.

### output_format_pretty_max_column_pad_width {#output_format_pretty_max_column_pad_width}

Maximum width to pad all values in a column in Pretty formats.

Default value: `250`.

### output_format_pretty_max_value_width {#output_format_pretty_max_value_width}

Limits the width of value displayed in [Pretty](../../interfaces/formats.md/#pretty) formats. If the value width exceeds the limit, the value is cut.

Possible values:

- Positive integer.
- 0 — The value is cut completely.

Default value: `10000` symbols.

**Examples**

Query:
```sql
SET output_format_pretty_max_value_width = 10;
SELECT range(number) FROM system.numbers LIMIT 10 FORMAT PrettyCompactNoEscapes;
```
Result:
```text
┌─range(number)─┐
│ []            │
│ [0]           │
│ [0,1]         │
│ [0,1,2]       │
│ [0,1,2,3]     │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
└───────────────┘
```

Query with zero width:
```sql
SET output_format_pretty_max_value_width = 0;
SELECT range(number) FROM system.numbers LIMIT 5 FORMAT PrettyCompactNoEscapes;
```
Result:
```text
┌─range(number)─┐
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
└───────────────┘
```

### output_format_pretty_color {#output_format_pretty_color}

Use ANSI escape sequences to paint colors in Pretty formats.

possible values:

-   `0` — Disabled. Pretty formats do not use ANSI escape sequences.
-   `1` — Enabled. Pretty formats will use ANSI escape sequences except for `NoEscapes` formats.
-   `auto` - Enabled if `stdout` is a terminal except for `NoEscapes` formats.

Default value is `auto`.

### output_format_pretty_grid_charset {#output_format_pretty_grid_charset}

Allows changing a charset which is used for printing grids borders. Available charsets are UTF-8, ASCII.

**Example**

``` text
SET output_format_pretty_grid_charset = 'UTF-8';
SELECT * FROM a;
┌─a─┐
│ 1 │
└───┘

SET output_format_pretty_grid_charset = 'ASCII';
SELECT * FROM a;
+-a-+
| 1 |
+---+
```

### output_format_pretty_row_numbers {#output_format_pretty_row_numbers}

Adds row numbers to output in the [Pretty](../../interfaces/formats.md/#pretty) format.

Possible values:

- 0 — Output without row numbers.
- 1 — Output with row numbers.

Default value: `1`.

**Example**

Query:

```sql
SET output_format_pretty_row_numbers = 1;
SELECT TOP 3 name, value FROM system.settings;
```

Result:
```text
   ┌─name────────────────────┬─value───┐
1. │ min_compress_block_size │ 65536   │
2. │ max_compress_block_size │ 1048576 │
3. │ max_block_size          │ 65505   │
   └─────────────────────────┴─────────┘
```

### output_format_pretty_single_large_number_tip_threshold {#output_format_pretty_single_large_number_tip_threshold}

Print a readable number tip on the right side of the table if the block consists of a single number which exceeds
this value (except 0).

Possible values:

- 0 — The readable number tip will not be printed.
- Positive integer — The readable number tip will be printed if the single number exceeds this value.

Default value: `1000000`.

**Example**

Query:

```sql
SELECT 1000000000 as a;
```

Result:
```text
┌──────────a─┐
│ 1000000000 │ -- 1.00 billion
└────────────┘
```

## Template format settings {#template-format-settings}

### format_template_resultset {#format_template_resultset}

Path to file which contains format string for result set (for Template format).

### format_template_resultset_format {#format_template_resultset_format}

Format string for result set (for Template format)

### format_template_row {#format_template_row}

Path to file which contains format string for rows (for Template format).

### format_template_rows_between_delimiter {#format_template_rows_between_delimiter}

Delimiter between rows (for Template format).

### format_template_row_format {#format_template_row_format}

Format string for rows (for Template format)

## CustomSeparated format settings {custom-separated-format-settings}

### format_custom_escaping_rule {#format_custom_escaping_rule}

Sets the field escaping rule for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Possible values:

- `'Escaped'` — Similarly to [TSV](../../interfaces/formats.md/#tabseparated).
- `'Quoted'` — Similarly to [Values](../../interfaces/formats.md/#data-format-values).
- `'CSV'` — Similarly to [CSV](../../interfaces/formats.md/#csv).
- `'JSON'` — Similarly to [JSONEachRow](../../interfaces/formats.md/#jsoneachrow).
- `'XML'` — Similarly to [XML](../../interfaces/formats.md/#xml).
- `'Raw'` — Extracts subpatterns as a whole, no escaping rules, similarly to [TSVRaw](../../interfaces/formats.md/#tabseparatedraw).

Default value: `'Escaped'`.

### format_custom_field_delimiter {#format_custom_field_delimiter}

Sets the character that is interpreted as a delimiter between the fields for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `'\t'`.

### format_custom_row_before_delimiter {#format_custom_row_before_delimiter}

Sets the character that is interpreted as a delimiter before the field of the first column for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `''`.

### format_custom_row_after_delimiter {#format_custom_row_after_delimiter}

Sets the character that is interpreted as a delimiter after the field of the last column for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `'\n'`.

### format_custom_row_between_delimiter {#format_custom_row_between_delimiter}

Sets the character that is interpreted as a delimiter between the rows for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `''`.

### format_custom_result_before_delimiter {#format_custom_result_before_delimiter}

Sets the character that is interpreted as a prefix before the result set for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `''`.

### format_custom_result_after_delimiter {#format_custom_result_after_delimiter}

Sets the character that is interpreted as a suffix after the result set for [CustomSeparated](../../interfaces/formats.md/#format-customseparated) data format.

Default value: `''`.

### input_format_custom_skip_trailing_empty_lines {input_format_custom_skip_trailing_empty_lines}

When enabled, trailing empty lines at the end of file in CustomSeparated format will be skipped.

Disabled by default.

### input_format_custom_allow_variable_number_of_columns {#input_format_custom_allow_variable_number_of_columns}

Allow variable number of columns in rows in CustomSeparated input format.
Ignore extra columns in rows with more columns than expected and treat missing columns as default values.

Disabled by default.

## Regexp format settings {#regexp-format-settings}

### format_regexp_escaping_rule {#format_regexp_escaping_rule}

Field escaping rule.

Possible values:

- `'Escaped'` — Similarly to [TSV](../../interfaces/formats.md/#tabseparated).
- `'Quoted'` — Similarly to [Values](../../interfaces/formats.md/#data-format-values).
- `'CSV'` — Similarly to [CSV](../../interfaces/formats.md/#csv).
- `'JSON'` — Similarly to [JSONEachRow](../../interfaces/formats.md/#jsoneachrow).
- `'XML'` — Similarly to [XML](../../interfaces/formats.md/#xml).
- `'Raw'` — Extracts subpatterns as a whole, no escaping rules, similarly to [TSVRaw](../../interfaces/formats.md/#tabseparatedraw).

Default value: `Raw`.

### format_regexp_skip_unmatched {#format_regexp_skip_unmatched}

Skip lines unmatched by regular expression.

Disabled by default.

## CapnProto format settings {#capn-proto-format-settings}

### format_capn_proto_enum_comparising_mode {#format_capn_proto_enum_comparising_mode}

Determines how to map ClickHouse `Enum` data type and [CapnProto](../../interfaces/formats.md/#capnproto) `Enum` data type from schema.

Possible values:

- `'by_values'` — Values in enums should be the same, names can be different.
- `'by_names'` — Names in enums should be the same, values can be different.
- `'by_name_case_insensitive'` — Names in enums should be the same case-insensitive, values can be different.

Default value: `'by_values'`.

### format_capn_proto_use_autogenerated_schema {#format_capn_proto_use_autogenerated_schema}

Use autogenerated CapnProto schema when [format_schema](#formatschema-format-schema) is not set.
The schema is generated from ClickHouse table structure using function [structureToCapnProtoSchema](../../sql-reference/functions/other-functions.md#structure_to_capnproto_schema)

## MySQLDump format settings {#musqldump-format-settings}

### input_format_mysql_dump_table_name (#input_format_mysql_dump_table_name)

The name of the table from which to read data from in MySQLDump input format.

### input_format_mysql_dump_map_columns (#input_format_mysql_dump_map_columns)

Enables matching columns from table in MySQL dump and columns from ClickHouse table by names in MySQLDump input format.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## SQLInsert format settings {#sqlinsert-format-settings}

### output_format_sql_insert_max_batch_size {#output_format_sql_insert_max_batch_size}

The maximum number of rows in one INSERT statement.

Default value: `65505`.

### output_format_sql_insert_table_name {#output_format_sql_insert_table_name}

The name of table that will be used in the output INSERT statement.

Default value: `table`.

### output_format_sql_insert_include_column_names {#output_format_sql_insert_include_column_names}

Include column names in INSERT statement.

Default value: `true`.

### output_format_sql_insert_use_replace {#output_format_sql_insert_use_replace}

Use REPLACE keyword instead of INSERT.

Default value: `false`.

### output_format_sql_insert_quote_names {#output_format_sql_insert_quote_names}

Quote column names with "`" characters

Default value: `true`.

## BSONEachRow format settings {#bson-each-row-format-settings}

### output_format_bson_string_as_string {#output_format_bson_string_as_string}

Use BSON String type instead of Binary for String columns.

Disabled by default.

### input_format_bson_skip_fields_with_unsupported_types_in_schema_inference {#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference}

Allow skipping columns with unsupported types while schema inference for format BSONEachRow.

Disabled by default.

## RowBinary format settings {#row-binary-format-settings}

### format_binary_max_string_size {#format_binary_max_string_size}

The maximum allowed size for String in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit.

Default value: `1GiB`.

## Native format settings {#native-format-settings}

### input_format_native_allow_types_conversion {#input_format_native_allow_types_conversion}

Allow types conversion in Native input format between columns from input data and requested columns.

Enabled by default.
