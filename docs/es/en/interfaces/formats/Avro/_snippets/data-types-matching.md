---
title: Correspondencia entre tipos de datos
---

La siguiente tabla muestra todos los tipos de datos compatibles con el formato Apache Avro y sus correspondientes [tipos de datos](/es/sql-reference/data-types/index.md) en ClickHouse para las consultas `INSERT` y `SELECT`.

| Tipo de datos de Avro `INSERT`              | Tipo de datos de ClickHouse                                                                                   | Tipo de datos de Avro `SELECT` |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------ |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/es/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/es/sql-reference/data-types/int-uint.md) | `int`                          |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/es/sql-reference/data-types/int-uint.md), [UInt64](/es/sql-reference/data-types/int-uint.md)               | `long`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/es/sql-reference/data-types/float.md)                                                                 | `float`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/es/sql-reference/data-types/float.md)                                                                 | `double`                       |
| `bytes`, `string`, `fixed`, `enum`          | [String](/es/sql-reference/data-types/string.md)                                                                 | `bytes` o `string` *           |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/es/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                     |
| `enum`                                      | [Enum(8\16)](/es/sql-reference/data-types/enum.md)                                                               | `enum`                         |
| `array(T)`                                  | [Array(T)](/es/sql-reference/data-types/array.md)                                                                | `array(T)`                     |
| `map(V, K)`                                 | [Map(V, K)](/es/sql-reference/data-types/map.md)                                                                 | `map(string, K)`               |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/es/sql-reference/data-types/date.md)                                                              | `union(null, T)`               |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/es/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **          |
| `null`                                      | [Nullable(Nothing)](/es/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                         |
| `int (date)` ***                            | [Date](/es/sql-reference/data-types/date.md), [Date32](/es/sql-reference/data-types/date32.md)                      | `int (date)` ***               |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/es/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` ***  |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/es/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` ***  |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/es/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***         |
| `int`                                       | [IPv4](/es/sql-reference/data-types/ipv4.md)                                                                     | `int`                          |
| `fixed(16)`                                 | [IPv6](/es/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                    |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/es/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***          |
| `string (uuid)` ***                         | [UUID](/es/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***            |
| `fixed(16)`                                 | [Int128/UInt128](/es/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                    |
| `fixed(32)`                                 | [Int256/UInt256](/es/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                    |
| `record`                                    | [Tuple](/es/sql-reference/data-types/tuple.md)                                                                   | `record`                       |

* `bytes` es el valor predeterminado y se controla mediante la configuración [`output_format_avro_string_column_pattern`](/es/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

**  El [tipo Variant](/es/sql-reference/data-types/variant) acepta implícitamente `null` como valor de un campo, por lo que, por ejemplo, el Avro `union(T1, T2, null)` se convertirá en `Variant(T1, T2)`.
Como resultado, al generar Avro desde ClickHouse, siempre tenemos que incluir el tipo `null` en el tipo `union` de Avro, ya que durante la inferencia de esquema no sabemos si algún valor es realmente `null`.

*** [Tipos lógicos de Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Tipos de datos lógicos de Avro no compatibles:

* `time-millis`
* `time-micros`
* `duration`