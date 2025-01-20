---
title : Avro
slug : /en/interfaces/formats/Avro
keywords : [Avro]
---

## Description

[Apache Avro](https://avro.apache.org/) is a row-oriented data serialization framework developed within Apache’s Hadoop project.
ClickHouse Avro format supports reading and writing [Avro data files](https://avro.apache.org/docs/current/spec.html#Object+Container+Files).

## Data Types Matching

The table below shows supported data types and how they match ClickHouse [data types](/docs/en/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                                                          | Avro data type `SELECT`         |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/docs/en/sql-reference/data-types/int-uint.md) | `int`                           |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/docs/en/sql-reference/data-types/int-uint.md), [UInt64](/docs/en/sql-reference/data-types/int-uint.md)               | `long`                          |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/docs/en/sql-reference/data-types/float.md)                                                                         | `float`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/docs/en/sql-reference/data-types/float.md)                                                                         | `double`                        |
| `bytes`, `string`, `fixed`, `enum`          | [String](/docs/en/sql-reference/data-types/string.md)                                                                         | `bytes` or `string` \*          |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/docs/en/sql-reference/data-types/fixedstring.md)                                                            | `fixed(N)`                      |
| `enum`                                      | [Enum(8\16)](/docs/en/sql-reference/data-types/enum.md)                                                                       | `enum`                          |
| `array(T)`                                  | [Array(T)](/docs/en/sql-reference/data-types/array.md)                                                                        | `array(T)`                      |
| `map(V, K)`                                 | [Map(V, K)](/docs/en/sql-reference/data-types/map.md)                                                                         | `map(string, K)`                |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/docs/en/sql-reference/data-types/date.md)                                                                      | `union(null, T)`                |
| `union(T1, T2, …)` \**                      | [Variant(T1, T2, …)](/docs/en/sql-reference/data-types/variant.md)                                                            | `union(T1, T2, …)` \**          |
| `null`                                      | [Nullable(Nothing)](/docs/en/sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                          |
| `int (date)` \**\*                          | [Date](/docs/en/sql-reference/data-types/date.md), [Date32](docs/en/sql-reference/data-types/date32.md)                       | `int (date)` \**\*              |
| `long (timestamp-millis)` \**\*             | [DateTime64(3)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \**\* |
| `long (timestamp-micros)` \**\*             | [DateTime64(6)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \**\* |
| `bytes (decimal)`  \**\*                    | [DateTime64(N)](/docs/en/sql-reference/data-types/datetime.md)                                                                | `bytes (decimal)`  \**\*        |
| `int`                                       | [IPv4](/docs/en/sql-reference/data-types/ipv4.md)                                                                             | `int`                           |
| `fixed(16)`                                 | [IPv6](/docs/en/sql-reference/data-types/ipv6.md)                                                                             | `fixed(16)`                     |
| `bytes (decimal)` \**\*                     | [Decimal(P, S)](/docs/en/sql-reference/data-types/decimal.md)                                                                 | `bytes (decimal)` \**\*         |
| `string (uuid)` \**\*                       | [UUID](/docs/en/sql-reference/data-types/uuid.md)                                                                             | `string (uuid)` \**\*           |
| `fixed(16)`                                 | [Int128/UInt128](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(16)`                     |
| `fixed(32)`                                 | [Int256/UInt256](/docs/en/sql-reference/data-types/int-uint.md)                                                               | `fixed(32)`                     |
| `record`                                    | [Tuple](/docs/en/sql-reference/data-types/tuple.md)                                                                           | `record`                        |


\* `bytes` is default, controlled by [output_format_avro_string_column_pattern](/docs/en/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

\**  [Variant type](/docs/en/sql-reference/data-types/variant) implicitly accepts `null` as a field value, so for example the Avro `union(T1, T2, null)` will be converted to `Variant(T1, T2)`.
As a result, when producing Avro from ClickHouse, we have to always include the `null` type to the Avro `union` type set as we don't know if any value is actually `null` during the schema inference.

\**\* [Avro logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Unsupported Avro logical data types: `time-millis`, `time-micros`, `duration`

## Example Usage

### Inserting Data

To insert data from an Avro file into ClickHouse table:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

The root schema of input Avro file must be of `record` type.

To find the correspondence between table columns and fields of Avro schema ClickHouse compares their names. This comparison is case-sensitive.
Unused fields are skipped.

Data types of ClickHouse table columns can differ from the corresponding fields of the Avro data inserted. When inserting data, ClickHouse interprets data types according to the table above and then [casts](/docs/en/sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) the data to corresponding column type.

While importing data, when field is not found in schema and setting [input_format_avro_allow_missing_fields](/docs/en/operations/settings/settings-formats.md/#input_format_avro_allow_missing_fields) is enabled, default value will be used instead of error.

### Selecting Data

To select data from ClickHouse table into an Avro file:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

Column names must:

- start with `[A-Za-z_]`
- subsequently contain only `[A-Za-z0-9_]`

Output Avro file compression and sync interval can be configured with [output_format_avro_codec](/docs/en/operations/settings/settings-formats.md/#output_format_avro_codec) and [output_format_avro_sync_interval](/docs/en/operations/settings/settings-formats.md/#output_format_avro_sync_interval) respectively.

### Example Data

Using the ClickHouse [DESCRIBE](/docs/en/sql-reference/statements/describe-table) function, you can quickly view the inferred format of an Avro file like the following example. This example includes the URL of a publicly accessible Avro file in the ClickHouse S3 public bucket:

```
DESCRIBE url('https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits.avro','Avro);
```
```
┌─name───────────────────────┬─type────────────┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ WatchID                    │ Int64           │              │                    │         │                  │                │
│ JavaEnable                 │ Int32           │              │                    │         │                  │                │
│ Title                      │ String          │              │                    │         │                  │                │
│ GoodEvent                  │ Int32           │              │                    │         │                  │                │
│ EventTime                  │ Int32           │              │                    │         │                  │                │
│ EventDate                  │ Date32          │              │                    │         │                  │                │
│ CounterID                  │ Int32           │              │                    │         │                  │                │
│ ClientIP                   │ Int32           │              │                    │         │                  │                │
│ ClientIP6                  │ FixedString(16) │              │                    │         │                  │                │
│ RegionID                   │ Int32           │              │                    │         │                  │                │
...
│ IslandID                   │ FixedString(16) │              │                    │         │                  │                │
│ RequestNum                 │ Int32           │              │                    │         │                  │                │
│ RequestTry                 │ Int32           │              │                    │         │                  │                │
└────────────────────────────┴─────────────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

## Format Settings