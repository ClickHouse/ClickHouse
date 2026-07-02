---
alias: []
description: 'Documentation for the Flatbuffers format'
input_format: false
keywords: ['Flatbuffers', 'FlexBuffers']
output_format: true
slug: /interfaces/formats/Flatbuffers
title: 'Flatbuffers'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `Flatbuffers` format serializes the result set as a single schema-less
[FlexBuffers](https://flatbuffers.dev/flexbuffers.html) value.

The root value is a vector of rows, and each row is a vector of the column values in the order of
the `SELECT`. The whole result is built in memory and written out at the end, so the format is not
streaming: keep this in mind when exporting very large result sets.

This format is only available when ClickHouse is built with the `flatbuffers` contrib library
(which is enabled together with Arrow); it is not available in the fast test build.

## Data types matching {#data-types-matching}

| ClickHouse data type                                                                                                          | FlexBuffers value      |
|-------------------------------------------------------------------------------------------------------------------------------|------------------------|
| [`UInt8`/`UInt16`/`UInt32`/`UInt64`](/sql-reference/data-types/int-uint.md), [`Date`](/sql-reference/data-types/date.md), [`DateTime`](/sql-reference/data-types/datetime.md) | `UInt`                 |
| [`Int8`/`Int16`/`Int32`/`Int64`](/sql-reference/data-types/int-uint.md), [`Date32`](/sql-reference/data-types/date32.md), [`DateTime64`](/sql-reference/data-types/datetime64.md) | `Int`                  |
| [`Enum8`/`Enum16`](/sql-reference/data-types/enum.md)                                                                          | `Int`                  |
| [`(U)Int128`/`(U)Int256`](/sql-reference/data-types/int-uint.md)                                                              | `Blob`                 |
| [`Float32`](/sql-reference/data-types/float.md)                                                                               | `Float`                |
| [`Float64`](/sql-reference/data-types/float.md)                                                                               | `Double`               |
| [`Decimal32`/`Decimal64`](/sql-reference/data-types/decimal.md)                                                               | `Int`                  |
| [`Decimal128`/`Decimal256`](/sql-reference/data-types/decimal.md)                                                             | `Blob`                 |
| [`String`](/sql-reference/data-types/string.md), [`FixedString`](/sql-reference/data-types/fixedstring.md)                    | `String`               |
| [`UUID`](/sql-reference/data-types/uuid.md)                                                                                   | `String` (text form)   |
| [`IPv4`](/sql-reference/data-types/ipv4.md)                                                                                   | `UInt`                 |
| [`IPv6`](/sql-reference/data-types/ipv6.md)                                                                                   | `Blob`                 |
| [`Array`](/sql-reference/data-types/array.md), [`Tuple`](/sql-reference/data-types/tuple.md)                                  | `Vector`               |
| [`Nullable`](/sql-reference/data-types/nullable.md) (`NULL`), [`Nothing`](/sql-reference/data-types/special-data-types/nothing.md) | `Null`             |
| [`LowCardinality`](/sql-reference/data-types/lowcardinality.md)                                                               | (the underlying value) |

A `Nullable` value that is not `NULL` is serialized as its underlying value.

Other types (for example [`Map`](/sql-reference/data-types/map.md)) are not supported and raise an
exception.

## Example usage {#example-usage}

Writing to a file ".fb":

```sql
$ clickhouse-client --query="SELECT number, toString(number) FROM numbers(10) FORMAT Flatbuffers" > tmp.fb;
```
