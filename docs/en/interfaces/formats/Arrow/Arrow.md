---
title : Arrow
slug : /en/interfaces/formats/Arrow
keywords : [Arrow]
---

## Description

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow’s "file mode" format. It is designed for in-memory random access.

## Data Types Matching

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/docs/en/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/docs/en/sql-reference/data-types/int-uint.md)/[Enum8](/docs/en/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/docs/en/sql-reference/data-types/int-uint.md)/[Enum16](/docs/en/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/docs/en/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/docs/en/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/docs/en/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/docs/en/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/docs/en/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/docs/en/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/docs/en/sql-reference/data-types/datetime64.md)                                              | `UINT32`                   |
| `STRING`, `BINARY`                      | [String](/docs/en/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/docs/en/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/docs/en/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/docs/en/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/docs/en/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/docs/en/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types also can be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [output_format_arrow_low_cardinality_as_dictionary](/docs/en/operations/settings/settings-formats.md/#output-format-arrow-low-cardinality-as-dictionary) setting that allows to output [LowCardinality](/docs/en/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type.

Unsupported Arrow data types: `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/en/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

## Example Usage

### Inserting Data

You can insert Arrow data from a file into ClickHouse table by the following command:

``` bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### Selecting Data

You can select data from a ClickHouse table and save them into some file in the Arrow format by the following command:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

## Format Settings 

- [output_format_arrow_low_cardinality_as_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_low_cardinality_as_dictionary) - enable output ClickHouse LowCardinality type as Dictionary Arrow type. Default value - `false`.
- [output_format_arrow_use_64_bit_indexes_for_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_use_64_bit_indexes_for_dictionary) - use 64-bit integer type for Dictionary indexes. Default value - `false`.
- [output_format_arrow_use_signed_indexes_for_dictionary](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_use_signed_indexes_for_dictionary) - use signed integer type for Dictionary indexes. Default value - `true`.
- [output_format_arrow_string_as_string](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_string_as_string) - use Arrow String type instead of Binary for String columns. Default value - `false`.
- [input_format_arrow_case_insensitive_column_matching](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching) - ignore case when matching Arrow columns with ClickHouse columns. Default value - `false`.
- [input_format_arrow_allow_missing_columns](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns) - allow missing columns while reading Arrow data. Default value - `false`.
- [input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference](/docs/en/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) - allow skipping columns with unsupported types while schema inference for Arrow format. Default value - `false`.
- [output_format_arrow_fixed_string_as_fixed_byte_array](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_fixed_string_as_fixed_byte_array) - use Arrow FIXED_SIZE_BINARY type instead of Binary/String for FixedString columns. Default value - `true`.
- [output_format_arrow_compression_method](/docs/en/operations/settings/settings-formats.md/#output_format_arrow_compression_method) - compression method used in output Arrow format. Default value - `lz4_frame`.