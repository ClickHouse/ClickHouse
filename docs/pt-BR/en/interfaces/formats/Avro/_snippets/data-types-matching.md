---
title: Correspondência entre tipos de dados
---

A tabela abaixo mostra todos os tipos de dados suportados pelo formato Apache Avro e os [tipos de dados](/pt-BR/sql-reference/data-types/index.md) correspondentes no ClickHouse em consultas `INSERT` e `SELECT`.

| Tipo de dados Avro `INSERT`                 | Tipo de dados ClickHouse                                                                                      | Tipo de dados Avro `SELECT`   |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/pt-BR/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/pt-BR/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/pt-BR/sql-reference/data-types/int-uint.md), [UInt64](/pt-BR/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/pt-BR/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/pt-BR/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/pt-BR/sql-reference/data-types/string.md)                                                                 | `bytes` ou `string` *         |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/pt-BR/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/pt-BR/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/pt-BR/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/pt-BR/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/pt-BR/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/pt-BR/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/pt-BR/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/pt-BR/sql-reference/data-types/date.md), [Date32](/pt-BR/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/pt-BR/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/pt-BR/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/pt-BR/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/pt-BR/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/pt-BR/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/pt-BR/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/pt-BR/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/pt-BR/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/pt-BR/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/pt-BR/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* `bytes` é o padrão, controlado pela configuração [`output_format_avro_string_column_pattern`](/pt-BR/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

**  O [tipo Variant](/pt-BR/sql-reference/data-types/variant) aceita implicitamente `null` como valor de campo; assim, por exemplo, o Avro `union(T1, T2, null)` será convertido em `Variant(T1, T2)`.
Como resultado, ao gerar Avro a partir do ClickHouse, precisamos sempre incluir o tipo `null` no conjunto de tipos `union` do Avro, pois durante a inferência de esquema não sabemos se algum valor é de fato `null`.

*** [tipos lógicos do Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Tipos de dados lógicos do Avro sem suporte:

* `time-millis`
* `time-micros`
* `duration`