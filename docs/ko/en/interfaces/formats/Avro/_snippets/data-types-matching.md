---
title: 데이터 타입 매핑
---

아래 표는 Apache Avro 포맷에서 지원하는 모든 데이터 타입과, `INSERT` 및 `SELECT` 쿼리에서 각각에 대응하는 ClickHouse [데이터 타입](/ko/sql-reference/data-types/index.md)을 보여줍니다.

| Avro 데이터 타입 `INSERT`                        | ClickHouse 데이터 타입                                                                                             | Avro 데이터 타입 `SELECT`          |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/ko/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/ko/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/ko/sql-reference/data-types/int-uint.md), [UInt64](/ko/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/ko/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/ko/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/ko/sql-reference/data-types/string.md)                                                                 | `bytes` 또는 `string` *         |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/ko/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/ko/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/ko/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/ko/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/ko/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/ko/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/ko/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/ko/sql-reference/data-types/date.md), [Date32](/ko/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/ko/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/ko/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/ko/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/ko/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/ko/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/ko/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/ko/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/ko/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/ko/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/ko/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* `bytes`가 기본값이며, 설정 [`output_format_avro_string_column_pattern`](/ko/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)으로 제어됩니다.

**  [Variant 타입](/ko/sql-reference/data-types/variant)은 필드 값으로 `null`을 암묵적으로 허용하므로, 예를 들어 Avro `union(T1, T2, null)`은 `Variant(T1, T2)`로 변환됩니다.
따라서 ClickHouse에서 Avro를 생성할 때는 스키마 추론 과정에서 실제로 `null`인 값이 있는지 알 수 없으므로, Avro `union` 타입 집합에 항상 `null` 타입을 포함해야 합니다.

*** [Avro 논리 타입](https://avro.apache.org/docs/current/spec.html#Logical+Types)

지원되지 않는 Avro 논리 데이터 타입:

* `time-millis`
* `time-micros`
* `duration`