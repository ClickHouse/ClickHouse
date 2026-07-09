---
title: Correspondance des types de données
---

Le tableau ci-dessous présente tous les types de données pris en charge par le format Apache Avro, ainsi que leurs [types de données](/fr/sql-reference/data-types/index.md) ClickHouse correspondants dans les requêtes `INSERT` et `SELECT`.

| Type de données Avro `INSERT`               | Type de données ClickHouse                                                                                    | Type de données Avro `SELECT` |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/fr/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/fr/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/fr/sql-reference/data-types/int-uint.md), [UInt64](/fr/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/fr/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/fr/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/fr/sql-reference/data-types/string.md)                                                                 | `bytes` ou `string` *         |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/fr/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/fr/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/fr/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/fr/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/fr/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/fr/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/fr/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/fr/sql-reference/data-types/date.md), [Date32](/fr/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/fr/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/fr/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/fr/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/fr/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/fr/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/fr/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/fr/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/fr/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/fr/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/fr/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* `bytes` est la valeur par défaut, contrôlée par le paramètre [`output_format_avro_string_column_pattern`](/fr/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

**  Le [type Variant](/fr/sql-reference/data-types/variant) accepte implicitement `null` comme valeur de champ ; ainsi, par exemple, l’Avro `union(T1, T2, null)` sera converti en `Variant(T1, T2)`.
Par conséquent, lors de la génération d’Avro à partir de ClickHouse, nous devons toujours inclure le type `null` dans l’ensemble des types de l’`union` Avro, car nous ne savons pas, pendant l’inférence de schéma, si une valeur est effectivement `null`.

*** [Types logiques Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Types de données logiques Avro non pris en charge :

* `time-millis`
* `time-micros`
* `duration`