---
title: データ型の対応
---

以下の表は、Apache Avro フォーマットでサポートされているすべてのデータ型と、`INSERT` クエリおよび `SELECT` クエリで対応する ClickHouse の[データ型](/ja/sql-reference/data-types/index.md)を示しています。

| Avro データ型 `INSERT`                          | ClickHouse データ型                                                                                               | Avro データ型 `SELECT`            |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/ja/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/ja/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/ja/sql-reference/data-types/int-uint.md), [UInt64](/ja/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/ja/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/ja/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/ja/sql-reference/data-types/string.md)                                                                 | `bytes` or `string` *         |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/ja/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/ja/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/ja/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/ja/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/ja/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/ja/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/ja/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/ja/sql-reference/data-types/date.md), [Date32](/ja/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/ja/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/ja/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/ja/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/ja/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/ja/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/ja/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/ja/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/ja/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/ja/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/ja/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* `bytes` がデフォルトで、設定 [`output_format_avro_string_column_pattern`](/ja/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern) で制御されます

**  [Variant 型](/ja/sql-reference/data-types/variant) は `null` をフィールド値として暗黙的に受け入れるため、たとえば Avro の `union(T1, T2, null)` は `Variant(T1, T2)` に変換されます。
そのため、ClickHouse から Avro を生成する際は、スキーマ推論の時点では実際に `null` の値が存在するかどうか分からないため、Avro の `union` 型には常に `null` 型を含める必要があります。

*** [Avro の論理型](https://avro.apache.org/docs/current/spec.html#Logical+Types)

サポートされていない Avro の論理データ型:

* `time-millis`
* `time-micros`
* `duration`