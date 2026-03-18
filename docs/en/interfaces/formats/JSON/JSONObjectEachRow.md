---
alias: []
description: 'Documentation for the JSONObjectEachRow format'
input_format: true
keywords: ['JSONObjectEachRow']
output_format: true
slug: /interfaces/formats/JSONObjectEachRow
title: 'JSONObjectEachRow'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

In this format, all data is represented as a single JSON Object, with each row represented as a separate field of this object similar to the [`JSONEachRow`](./JSONEachRow.md) format.

## Example Usage {#example-usage}

### Basic Example {#basic-example}

Given some JSON:

```json
{
  "row_1": {"num": 42, "str": "hello", "arr":  [0,1]},
  "row_2": {"num": 43, "str": "hello", "arr":  [0,1,2]},
  "row_3": {"num": 44, "str": "hello", "arr":  [0,1,2,3]}
}
```

To use an object name as a column value you can use the special setting [`format_json_object_each_row_column_for_object_name`](/operations/settings/settings-formats.md/#format_json_object_each_row_column_for_object_name). 
The value of this setting is set to the name of a column, that is used as JSON key for a row in the resulting object.

#### Output {#output}

Let's say we have the table `test` with two columns:

```text
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

Let's output it in the `JSONObjectEachRow` format and use the `format_json_object_each_row_column_for_object_name` setting:

```sql title="Query"
SELECT * FROM test SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```json title="Response"
{
    "first_obj": {"number": 1},
    "second_obj": {"number": 2},
    "third_obj": {"number": 3}
}
```

#### Input {#input}

Let's say we stored the output from the previous example in a file named `data.json`:

```sql title="Query"
SELECT * FROM file('data.json', JSONObjectEachRow, 'object_name String, number UInt64') SETTINGS format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─object_name─┬─number─┐
│ first_obj   │      1 │
│ second_obj  │      2 │
│ third_obj   │      3 │
└─────────────┴────────┘
```

It also works for schema inference:

```sql title="Query"
DESCRIBE file('data.json', JSONObjectEachRow) SETTING format_json_object_each_row_column_for_object_name='object_name'
```

```response title="Response"
┌─name────────┬─type────────────┐
│ object_name │ String          │
│ number      │ Nullable(Int64) │
└─────────────┴─────────────────┘
```


### Inserting Data {#json-inserting-data}

```sql title="Query"
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

ClickHouse allows:

- Any order of key-value pairs in the object.
- Omitting some values.

ClickHouse ignores spaces between elements and commas after the objects. You can pass all the objects in one line. You do not have to separate them with line breaks.

#### Omitted values processing {#omitted-values-processing}

ClickHouse substitutes omitted values with the default values for the corresponding [data types](/sql-reference/data-types/index.md).

If `DEFAULT expr` is specified, ClickHouse uses different substitution rules depending on the [input_format_defaults_for_omitted_fields](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) setting.

Consider the following table:

```sql title="Query"
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

- If `input_format_defaults_for_omitted_fields = 0`, then the default value for `x` and `a` equals `0` (as the default value for the `UInt32` data type).
- If `input_format_defaults_for_omitted_fields = 1`, then the default value for `x` equals `0`, but the default value of `a` equals `x * 2`.

:::note
When inserting data with `input_format_defaults_for_omitted_fields = 1`, ClickHouse consumes more computational resources, compared to insertion with `input_format_defaults_for_omitted_fields = 0`.
:::

### Selecting Data {#json-selecting-data}

Consider the `UserActivity` table as an example:

```response
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

The query `SELECT * FROM UserActivity FORMAT JSONEachRow` returns:

```response
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

Unlike the [JSON](/interfaces/formats/JSON) format, there is no substitution of invalid UTF-8 sequences. Values are escaped in the same way as for `JSON`.

:::info
Any set of bytes can be output in the strings. Use the [`JSONEachRow`](./JSONEachRow.md) format if you are sure that the data in the table can be formatted as JSON without losing any information.
:::

### Usage of Nested Structures {#jsoneachrow-nested}

If you have a table with the [`Nested`](/sql-reference/data-types/nested-data-structures/index.md) data type columns, you can insert JSON data with the same structure. Enable this feature with the [input_format_import_nested_json](/operations/settings/settings-formats.md/#input_format_import_nested_json) setting.

For example, consider the following table:

```sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

As you can see in the `Nested` data type description, ClickHouse treats each component of the nested structure as a separate column (`n.s` and `n.i` for our table). You can insert data in the following way:

```sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

To insert data as a hierarchical JSON object, set [`input_format_import_nested_json=1`](/operations/settings/settings-formats.md/#input_format_import_nested_json).

```json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

Without this setting, ClickHouse throws an exception.

```sql title="Query"
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

```response title="Response"
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

```sql title="Query"
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

```response title="Response"
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

```sql title="Query"
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

```response title="Response"
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## Format Settings {#format-settings}

| Setting                                                                                                                                                                            | Description                                                                                                                                                             | Default  | Notes                                                                                                                                                                                         |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`input_format_import_nested_json`](/operations/settings/settings-formats.md/#input_format_import_nested_json)                                                              | map nested JSON data to nested tables (it works for JSONEachRow format).                                                                                                | `false`  |                                                                                                                                                                                               |
| [`input_format_json_read_bools_as_numbers`](/operations/settings/settings-formats.md/#input_format_json_read_bools_as_numbers)                                              | allow to parse bools as numbers in JSON input formats.                                                                                                                  | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_bools_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_bools_as_strings)                                              | allow to parse bools as strings in JSON input formats.                                                                                                                  | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_numbers_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_numbers_as_strings)                                          | allow to parse numbers as strings in JSON input formats.                                                                                                                | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_arrays_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_arrays_as_strings)                                            | allow to parse JSON arrays as strings in JSON input formats.                                                                                                            | `true`   |                                                                                                                                                                                               |
| [`input_format_json_read_objects_as_strings`](/operations/settings/settings-formats.md/#input_format_json_read_objects_as_strings)                                          | allow to parse JSON objects as strings in JSON input formats.                                                                                                           | `true`   |                                                                                                                                                                                               |
| [`input_format_json_named_tuples_as_objects`](/operations/settings/settings-formats.md/#input_format_json_named_tuples_as_objects)                                          | parse named tuple columns as JSON objects.                                                                                                                              | `true`   |                                                                                                                                                                                               |
| [`input_format_json_try_infer_numbers_from_strings`](/operations/settings/settings-formats.md/#input_format_json_try_infer_numbers_from_strings)                            | try to infer numbers from string fields while schema inference.                                                                                                         | `false`  |                                                                                                                                                                                               |
| [`input_format_json_try_infer_named_tuples_from_objects`](/operations/settings/settings-formats.md/#input_format_json_try_infer_named_tuples_from_objects)                  | try to infer named tuple from JSON objects during schema inference.                                                                                                     | `true`   |                                                                                                                                                                                               |
| [`input_format_json_infer_incomplete_types_as_strings`](/operations/settings/settings-formats.md/#input_format_json_infer_incomplete_types_as_strings)                      | use type String for keys that contains only Nulls or empty objects/arrays during schema inference in JSON input formats.                                                | `true`   |                                                                                                                                                                                               |
| [`input_format_json_defaults_for_missing_elements_in_named_tuple`](/operations/settings/settings-formats.md/#input_format_json_defaults_for_missing_elements_in_named_tuple) | insert default values for missing elements in JSON object while parsing named tuple.                                                                                   | `true`   |                                                                                                                                                                                               |
| [`input_format_json_ignore_unknown_keys_in_named_tuple`](/operations/settings/settings-formats.md/#input_format_json_ignore_unknown_keys_in_named_tuple)                    | ignore unknown keys in json object for named tuples.                                                                                                                    | `false`  |                                                                                                                                                                                               |
| [`input_format_json_compact_allow_variable_number_of_columns`](/operations/settings/settings-formats.md/#input_format_json_compact_allow_variable_number_of_columns)        | allow variable number of columns in JSONCompact/JSONCompactEachRow format, ignore extra columns and use default values on missing columns.                              | `false`  |                                                                                                                                                                                               |
| [`input_format_json_throw_on_bad_escape_sequence`](/operations/settings/settings-formats.md/#input_format_json_throw_on_bad_escape_sequence)                                | throw an exception if JSON string contains bad escape sequence. If disabled, bad escape sequences will remain as is in the data.                                        | `true`   |                                                                                                                                                                                               |
| [`input_format_json_empty_as_default`](/operations/settings/settings-formats.md/#input_format_json_empty_as_default)                                                        | treat empty fields in JSON input as default values.                                                                                                                     | `false`. | For complex default expressions [`input_format_defaults_for_omitted_fields`](/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields) must be enabled too. |
| [`output_format_json_quote_64bit_integers`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers)                                              | controls quoting of 64-bit integers in JSON output format.                                                                                                              | `true`   |                                                                                                                                                                                               |
| [`output_format_json_quote_64bit_floats`](/operations/settings/settings-formats.md/#output_format_json_quote_64bit_floats)                                                  | controls quoting of 64-bit floats in JSON output format.                                                                                                                | `false`  |                                                                                                                                                                                               |
| [`output_format_json_quote_denormals`](/operations/settings/settings-formats.md/#output_format_json_quote_denormals)                                                        | enables '+nan', '-nan', '+inf', '-inf' outputs in JSON output format.                                                                                                   | `false`  |                                                                                                                                                                                               |
| [`output_format_json_quote_decimals`](/operations/settings/settings-formats.md/#output_format_json_quote_decimals)                                                          | controls quoting of decimals in JSON output format.                                                                                                                     | `false`  |                                                                                                                                                                                               |
| [`output_format_json_escape_forward_slashes`](/operations/settings/settings-formats.md/#output_format_json_escape_forward_slashes)                                          | controls escaping forward slashes for string outputs in JSON output format.                                                                                             | `true`   |                                                                                                                                                                                               |
| [`output_format_json_named_tuples_as_objects`](/operations/settings/settings-formats.md/#output_format_json_named_tuples_as_objects)                                        | serialize named tuple columns as JSON objects.                                                                                                                          | `true`   |                                                                                                                                                                                               |
| [`output_format_json_array_of_rows`](/operations/settings/settings-formats.md/#output_format_json_array_of_rows)                                                            | output a JSON array of all rows in JSONEachRow(Compact) format.                                                                                                         | `false`  |                                                                                                                                                                                               |
| [`output_format_json_validate_utf8`](/operations/settings/settings-formats.md/#output_format_json_validate_utf8)                                                            | enables validation of UTF-8 sequences in JSON output formats (note that it doesn't impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8). | `false`  |                                                                                                                                                                                               |