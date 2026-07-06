---
alias: []
description: 'Documentation for the Arrow format'
input_format: true
keywords: ['Arrow']
output_format: true
slug: /interfaces/formats/Arrow
title: 'Arrow'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Apache Arrow](https://arrow.apache.org/) comes with two built-in columnar storage formats.
ClickHouse supports read and write operations for these formats.
`Arrow` is Apache Arrow's "file mode" format, designed for in-memory random access.

## Data types matching {#data-types-matching}

The table below shows the supported data types and how they correspond to ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Arrow data type (`INSERT`)              | ClickHouse data type                                                                                       | Arrow data type (`SELECT`) |
|-----------------------------------------|------------------------------------------------------------------------------------------------------------|----------------------------|
| `BOOL`                                  | [Bool](/sql-reference/data-types/boolean.md)                                                       | `BOOL`                     |
| `UINT8`, `BOOL`                         | [UInt8](/sql-reference/data-types/int-uint.md)                                                     | `UINT8`                    |
| `INT8`                                  | [Int8](/sql-reference/data-types/int-uint.md)/[Enum8](/sql-reference/data-types/enum.md)   | `INT8`                     |
| `UINT16`                                | [UInt16](/sql-reference/data-types/int-uint.md)                                                    | `UINT16`                   |
| `INT16`                                 | [Int16](/sql-reference/data-types/int-uint.md)/[Enum16](/sql-reference/data-types/enum.md) | `INT16`                    |
| `UINT32`                                | [UInt32](/sql-reference/data-types/int-uint.md)                                                    | `UINT32`                   |
| `INT32`                                 | [Int32](/sql-reference/data-types/int-uint.md)                                                     | `INT32`                    |
| `UINT64`                                | [UInt64](/sql-reference/data-types/int-uint.md)                                                    | `UINT64`                   |
| `INT64`                                 | [Int64](/sql-reference/data-types/int-uint.md)                                                     | `INT64`                    |
| `FLOAT`, `HALF_FLOAT`                   | [Float32](/sql-reference/data-types/float.md)                                                      | `FLOAT32`                  |
| `DOUBLE`                                | [Float64](/sql-reference/data-types/float.md)                                                      | `FLOAT64`                  |
| `DATE32`                                | [Date32](/sql-reference/data-types/date32.md)                                                      | `UINT16`                   |
| `DATE64`                                | [DateTime](/sql-reference/data-types/datetime.md)                                                  | `UINT32`                   |
| `TIMESTAMP`, `TIME32`, `TIME64`         | [DateTime64](/sql-reference/data-types/datetime64.md)                                              | `TIMESTAMP`                |
| `STRING`, `BINARY`                      | [String](/sql-reference/data-types/string.md)                                                      | `BINARY`                   |
| `STRING`, `BINARY`, `FIXED_SIZE_BINARY` | [FixedString](/sql-reference/data-types/fixedstring.md)                                            | `FIXED_SIZE_BINARY`        |
| `DECIMAL`                               | [Decimal](/sql-reference/data-types/decimal.md)                                                    | `DECIMAL`                  |
| `DECIMAL256`                            | [Decimal256](/sql-reference/data-types/decimal.md)                                                 | `DECIMAL256`               |
| `LIST`                                  | [Array](/sql-reference/data-types/array.md)                                                        | `LIST`                     |
| `STRUCT`                                | [Tuple](/sql-reference/data-types/tuple.md)                                                        | `STRUCT`                   |
| `MAP`                                   | [Map](/sql-reference/data-types/map.md)                                                            | `MAP`                      |
| `UINT32`                                | [IPv4](/sql-reference/data-types/ipv4.md)                                                          | `UINT32`                   |
| `FIXED_SIZE_BINARY`, `BINARY`           | [IPv6](/sql-reference/data-types/ipv6.md)                                                          | `FIXED_SIZE_BINARY`        |
| `FIXED_SIZE_BINARY`, `BINARY`           | [Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md)                             | `FIXED_SIZE_BINARY`        |
| `DURATION`                              | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Nanosecond/Microsecond/Millisecond/Second) | `DURATION`    |
| `INT64`                                 | [Interval](/sql-reference/data-types/special-data-types/interval.md) (Minute/Hour/Day/Week/Month/Quarter/Year) | `INT64`         |

Arrays can be nested and can have a value of the `Nullable` type as an argument. `Tuple` and `Map` types can also be nested.

The `DICTIONARY` type is supported for `INSERT` queries, and for `SELECT` queries there is an [`output_format_arrow_low_cardinality_as_dictionary`](/operations/settings/formats#output_format_arrow_low_cardinality_as_dictionary) setting that allows to output [LowCardinality](/sql-reference/data-types/lowcardinality.md) type as a `DICTIONARY` type. Note that there might be unused values in `LowCardinality` dictionary, which can lead to unused values in Arrow `DICTIONARY` during output.

Unsupported Arrow data types: 
- `FIXED_SIZE_BINARY`
- `JSON`
- `UUID`
- `ENUM`.

The data types of ClickHouse table columns do not have to match the corresponding Arrow data fields. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/sql-reference/functions/type-conversion-functions#CAST) the data to the data type set for the ClickHouse table column.

## Example usage {#example-usage}

In the example below we use the `forex` dataset available in the
[ClickHouse SQL playground](https://sql.clickhouse.com).

### Selecting data {#selecting-data}

We select one day of `EUR/USD` exchange rates from the playground and save it
into a local `forex_eurusd.arrow` file. We query the playground over the HTTP
interface, where the host is `sql-clickhouse.clickhouse.com` and the user is
`demo` (which has no password):

```bash
curl "https://sql-clickhouse.clickhouse.com:8443/?user=demo&database=forex" \
    --data-binary "
        SELECT
            concat(base, '.', quote) AS base_quote,
            datetime AS last_update,
            CAST(bid, 'Float32') AS bid,
            CAST(ask, 'Float32') AS ask,
            ask - bid AS spread
        FROM forex
        WHERE base = 'EUR' AND quote = 'USD'
            AND datetime >= '2020-01-01' AND datetime < '2020-01-02'
        ORDER BY datetime ASC
        FORMAT Arrow
        SETTINGS output_format_arrow_compression_method='zstd'" > forex_eurusd.arrow
```

### Reading the file back {#reading-data}

We can now read the local Arrow file back with
[`clickhouse-local`](/operations/utilities/clickhouse-local) using the
[`file`](/sql-reference/table-functions/file) table function. The file is
self-describing, so the `Arrow` format infers the schema automatically:

```bash
clickhouse-local --query "
    SELECT *
    FROM file('forex_eurusd.arrow', Arrow)
    ORDER BY last_update ASC
    LIMIT 5
    FORMAT PrettyCompact"
```

```response title="Response"
   ┌─base_quote─┬─────────────last_update─┬─────bid─┬─────ask─┬────────────────spread─┐
1. │ EUR.USD    │ 2020-01-01 17:00:00.065 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
2. │ EUR.USD    │ 2020-01-01 17:00:10.447 │  1.1212 │ 1.12192 │ 0.0007200241088867188 │
3. │ EUR.USD    │ 2020-01-01 17:00:10.498 │ 1.12117 │ 1.12161 │ 0.0004400014877319336 │
4. │ EUR.USD    │ 2020-01-01 17:00:12.579 │  1.1212 │ 1.12161 │ 0.0004100799560546875 │
5. │ EUR.USD    │ 2020-01-01 17:00:12.630 │  1.1212 │ 1.12172 │ 0.0005199909210205078 │
   └────────────┴─────────────────────────┴─────────┴─────────┴───────────────────────┘
```

### Inserting data {#inserting-data}

To load an Arrow file into a ClickHouse table, pipe it into `clickhouse-client`
with `FORMAT Arrow`:

```bash
cat forex_eurusd.arrow | clickhouse-client --query="INSERT INTO some_table FORMAT Arrow"
```

## Format settings {#format-settings}

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
