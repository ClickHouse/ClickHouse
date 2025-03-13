---
title : Arrow
slug: /interfaces/formats/Arrow
keywords : [Arrow]
input_format: true
output_format: true
alias: []
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats. ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format. It is designed for in-memory random access.

## Data Types Matching

The table below shows the supported data types and how they correspond to ClickHouse [data types](/docs/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/docs/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/docs/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/docs/sql-reference/data-types/int-uint.md)/[Enum8](/docs/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/docs/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/docs/sql-reference/data-types/int-uint.md)/[Enum16](/docs/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/docs/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/docs/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/docs/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/docs/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/docs/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/docs/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/docs/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/docs/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/docs/sql-reference/data-types/datetime64.md)                                              | `UINT32`                   |
| `STRING`, `BINARY`                      | [String](/docs/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/docs/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/docs/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/docs/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/docs/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/docs/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/docs/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/docs/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/docs/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/docs/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/docs/operations/settings/settings-formats.md/#output-format-arrow-low-cardinality-as-dictionary) setting that allows to output [LowCardinality](/docs/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type.

Unsupported Arrow data types: 
- `FIXED_SIZE_BINARY`
- `JSON`
- `UUID`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to the data type set for the ClickHouse table column.

## Example Usage

### Inserting Data

You can insert Arrow data from a file into ClickHouse table using the following command:

```bash
$ cat filename.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

### Selecting Data

You can select data from a ClickHouse table and save it into some file in the Arrow format using the following command:

```bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Arrow" > {filename.arrow}
```

## Format Settings

| Setting                                                                                                                  | Description                                                                                        | Default      |
|--------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|--------------|
| `input_format_arrow_allow_missing_columns`                                                                               | Allow missing columns while reading Arrow input formats                                            | `1`          |
| `input_format_arrow_case_insensitive_column_matching`                                                                    | Ignore case when matching Arrow columns with CH columns.                                           | `0`          |
| `input_format_arrow_import_nested`                                                                                       | Obsolete setting, does nothing.                                                                    | `0`          |
| `input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`                                             | Skip columns with unsupported types while schema inference for format Arrow                        | `0`          |
| `output_format_arrow_compression_method`                                                                                 | Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed) | `lz4_frame`  |
| `output_format_arrow_fixed_string_as_fixed_byte_array`                                                                   | Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.                        | `1`          |
| `output_format_arrow_low_cardinality_as_dictionary`                                                                      | Enable output LowCardinality type as Dictionary Arrow type                                         | `0`          |
| `output_format_arrow_string_as_string`                                                                                   | Use Arrow String type instead of Binary for String columns                                         | `1`          |
| `output_format_arrow_use_64_bit_indexes_for_dictionary`                                                                  | Always use 64 bit integers for dictionary indexes in Arrow format                                  | `0`          |
| `output_format_arrow_use_signed_indexes_for_dictionary`                                                                  | Use signed integers for dictionary indexes in Arrow format                                         | `1`          |