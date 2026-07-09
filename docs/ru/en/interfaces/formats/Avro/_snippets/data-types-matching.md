---
title: Соответствие типов данных
---

В таблице ниже приведены все типы данных, поддерживаемые форматом Apache Avro, и соответствующие им [типы данных](/ru/sql-reference/data-types/index.md) ClickHouse в запросах `INSERT` и `SELECT`.

| Тип данных Avro для `INSERT`                | Тип данных ClickHouse                                                                                         | Тип данных Avro для `SELECT`  |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/ru/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/ru/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/ru/sql-reference/data-types/int-uint.md), [UInt64](/ru/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/ru/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/ru/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/ru/sql-reference/data-types/string.md)                                                                 | `bytes` или `string` *        |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/ru/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/ru/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/ru/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/ru/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/ru/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/ru/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/ru/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/ru/sql-reference/data-types/date.md), [Date32](/ru/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/ru/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/ru/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/ru/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/ru/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/ru/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/ru/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/ru/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/ru/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/ru/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/ru/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* Значение `bytes` используется по умолчанию и задается настройкой [`output_format_avro_string_column_pattern`](/ru/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

**  [Тип варианта](/ru/sql-reference/data-types/variant) неявно допускает `null` в качестве значения поля, поэтому, например, Avro `union(T1, T2, null)` будет преобразован в `Variant(T1, T2)`.
В результате при формировании Avro из ClickHouse нам всегда нужно включать тип `null` в набор типов Avro `union`, так как во время автоматического вывода схемы мы не знаем, является ли какое-либо значение `null`.

*** [Логические типы Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Неподдерживаемые логические типы данных Avro:

* `time-millis`
* `time-micros`
* `duration`