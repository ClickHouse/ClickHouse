---
alias: []
description: 'Documentation for the Vortex format'
input_format: true
keywords: ['Vortex']
output_format: true
slug: /interfaces/formats/Vortex
title: 'Vortex'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

[Vortex](https://vortex.dev/) is an extensible columnar file format for compressed Apache Arrow-compatible data,
designed for fast scans and random access. ClickHouse supports reading and writing Vortex files.

## Data types matching {#data-types-matching}

The table below shows the Vortex data types and the corresponding ClickHouse [data types](/sql-reference/data-types/index.md)
in `INSERT` and `SELECT` queries.

| Vortex data type (`INSERT`) | ClickHouse data type                                                                                   | Vortex data type (`SELECT`) |
|-----------------------------|--------------------------------------------------------------------------------------------------------|-----------------------------|
| `Bool`                      | [Bool](/sql-reference/data-types/boolean.md)                                                    | `Bool`                      |
| `I8`, `U8`                  | [Int8/UInt8](/sql-reference/data-types/int-uint.md)                                             | `I8`, `U8`                  |
| `I16`, `U16`                | [Int16/UInt16](/sql-reference/data-types/int-uint.md)                                           | `I16`, `U16`                |
| `I32`, `U32`                | [Int32/UInt32](/sql-reference/data-types/int-uint.md)                                           | `I32`, `U32`                |
| `I64`, `U64`                | [Int64/UInt64](/sql-reference/data-types/int-uint.md)                                           | `I64`, `U64`                |
| `F32`                       | [Float32](/sql-reference/data-types/float.md)                                                   | `F32`                       |
| `F64`                       | [Float64](/sql-reference/data-types/float.md)                                                   | `F64`                       |
| `Utf8`, `Binary`            | [String](/sql-reference/data-types/string.md)                                                   | `Binary`                    |
| `Binary`                    | [FixedString](/sql-reference/data-types/fixedstring.md)                                         | `Binary`                    |
| `Decimal`                   | [Decimal](/sql-reference/data-types/decimal.md)                                                 | `Decimal`                   |
| `vortex.date`               | [Date32](/sql-reference/data-types/date32.md)                                                   | `vortex.date`               |
| `vortex.timestamp`          | [DateTime64](/sql-reference/data-types/datetime64.md)                                           | `vortex.timestamp`          |
| `vortex.time`               | [Time64](/sql-reference/data-types/time64.md)                                                   | `vortex.time`               |
| `List`                      | [Array](/sql-reference/data-types/array.md)                                                     | `List`                      |
| `Struct`                    | [Tuple](/sql-reference/data-types/tuple.md)                                                     | `Struct`                    |
| `Null`                      | [Nothing](/sql-reference/data-types/special-data-types/nothing.md)                              | `Null`                      |

Other types are not supported. In particular, [Map](/sql-reference/data-types/map.md),
[Int128/UInt128/Int256/UInt256](/sql-reference/data-types/int-uint.md), [IPv6](/sql-reference/data-types/ipv6.md)
and [Interval](/sql-reference/data-types/special-data-types/interval.md) columns cannot be written to Vortex files.
[String](/sql-reference/data-types/string.md) columns are written as `Binary` because ClickHouse strings are
arbitrary byte sequences, while Vortex requires `Utf8` values to be valid UTF-8. Vortex has no
fixed-size binary type, so [FixedString](/sql-reference/data-types/fixedstring.md) is also written as `Binary`,
and [LowCardinality](/sql-reference/data-types/lowcardinality.md) columns are written as their underlying type
(Vortex chooses dictionary and other encodings adaptively by itself).

The data types of ClickHouse table columns do not have to match the corresponding Vortex data fields.
When inserting data, ClickHouse interprets data types according to the table above and then
[casts](/sql-reference/functions/type-conversion-functions.md#cast) the data to the data type set for the
ClickHouse table column.

## Example usage {#example-usage}

You can select data from a Vortex file:

```sql
SELECT * FROM file('data.vortex', Vortex);
```

And write data to a Vortex file:

```sql
SELECT * FROM numbers(3) INTO OUTFILE 'numbers.vortex' FORMAT Vortex;
```

## Format settings {#format-settings}

The format has no dedicated settings. As in other columnar formats, only the columns used by the
query are read from the file, and columns missing in the file are filled with default values.
