---
title: 数据类型匹配
---

下表显示了 Apache Avro 格式支持的所有数据类型，以及它们在 `INSERT` 和 `SELECT` 查询中对应的 ClickHouse [数据类型](/zh/sql-reference/data-types/index.md)。

| Avro 数据类型 `INSERT`                          | ClickHouse 数据类型                                                                                               | Avro 数据类型 `SELECT`            |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/zh/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/zh/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/zh/sql-reference/data-types/int-uint.md), [UInt64](/zh/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/zh/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/zh/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/zh/sql-reference/data-types/string.md)                                                                 | `bytes` 或 `string` *          |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/zh/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/zh/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/zh/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/zh/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/zh/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/zh/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/zh/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/zh/sql-reference/data-types/date.md), [Date32](/zh/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/zh/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/zh/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/zh/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/zh/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/zh/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/zh/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/zh/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/zh/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/zh/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/zh/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* `bytes` 为默认值，由设置 [`output_format_avro_string_column_pattern`](/zh/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern) 控制

**  [Variant 类型](/zh/sql-reference/data-types/variant) 会隐式接受 `null` 作为字段值，因此，例如 Avro 的 `union(T1, T2, null)` 会被转换为 `Variant(T1, T2)`。
因此，当从 ClickHouse 生成 Avro 时，我们必须始终将 `null` 类型包含在 Avro 的 `union` 类型集合中，因为在进行 schema inference 时，我们无法判断某个值是否实际为 `null`。

*** [Avro 逻辑类型](https://avro.apache.org/docs/current/spec.html#Logical+Types)

不支持的 Avro 逻辑数据类型：

* `time-millis`
* `time-micros`
* `duration`