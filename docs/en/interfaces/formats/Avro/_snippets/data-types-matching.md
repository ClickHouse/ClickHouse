The table below shows all data types supported by the Apache Avro format, and their corresponding ClickHouse [data types](/sql-reference/data-types/index.md) in `INSERT` and `SELECT` queries.

| Avro data type `INSERT`                     | ClickHouse data type                                                                                                          | Avro data type `SELECT`         |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|---------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/sql-reference/data-types/int-uint.md) | `int`                           |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/sql-reference/data-types/int-uint.md), [UInt64](/sql-reference/data-types/int-uint.md)               | `long`                          |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/sql-reference/data-types/float.md)                                                                         | `float`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/sql-reference/data-types/float.md)                                                                         | `double`                        |
| `bytes`, `string`, `fixed`, `enum`          | [String](/sql-reference/data-types/string.md)                                                                         | `bytes` or `string` \*          |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/sql-reference/data-types/fixedstring.md)                                                            | `fixed(N)`                      |
| `enum`                                      | [Enum(8\16)](/sql-reference/data-types/enum.md)                                                                       | `enum`                          |
| `array(T)`                                  | [Array(T)](/sql-reference/data-types/array.md)                                                                        | `array(T)`                      |
| `map(V, K)`                                 | [Map(V, K)](/sql-reference/data-types/map.md)                                                                         | `map(string, K)`                |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/sql-reference/data-types/date.md)                                                                      | `union(null, T)`                |
| `union(T1, T2, …)` \**                      | [Variant(T1, T2, …)](/sql-reference/data-types/variant.md)                                                            | `union(T1, T2, …)` \**          |
| `null`                                      | [Nullable(Nothing)](/sql-reference/data-types/special-data-types/nothing.md)                                          | `null`                          |
| `int (date)` \**\*                          | [Date](/sql-reference/data-types/date.md), [Date32](/sql-reference/data-types/date32.md)                       | `int (date)` \**\*              |
| `long (timestamp-millis)` \**\*             | [DateTime64(3)](/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \**\* |
| `long (timestamp-micros)` \**\*             | [DateTime64(6)](/sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \**\* |
| `bytes (decimal)`  \**\*                    | [DateTime64(N)](/sql-reference/data-types/datetime.md)                                                                | `bytes (decimal)`  \**\*        |
| `int`                                       | [IPv4](/sql-reference/data-types/ipv4.md)                                                                             | `int`                           |
| `fixed(16)`                                 | [IPv6](/sql-reference/data-types/ipv6.md)                                                                             | `fixed(16)`                     |
| `bytes (decimal)` \**\*                     | [Decimal(P, S)](/sql-reference/data-types/decimal.md)                                                                 | `bytes (decimal)` \**\*         |
| `string (uuid)` \**\*                       | [UUID](/sql-reference/data-types/uuid.md)                                                                             | `string (uuid)` \**\*           |
| `fixed(16)`                                 | [Int128/UInt128](/sql-reference/data-types/int-uint.md)                                                               | `fixed(16)`                     |
| `fixed(32)`                                 | [Int256/UInt256](/sql-reference/data-types/int-uint.md)                                                               | `fixed(32)`                     |
| `record`                                    | [Tuple](/sql-reference/data-types/tuple.md)                                                                           | `record`                        |

\* `bytes` is default, controlled by setting [`output_format_avro_string_column_pattern`](/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

\**  The [Variant type](/sql-reference/data-types/variant) implicitly accepts `null` as a field value, so for example the Avro `union(T1, T2, null)` will be converted to `Variant(T1, T2)`.
As a result, when producing Avro from ClickHouse, we have to always include the `null` type to the Avro `union` type set as we don't know if any value is actually `null` during the schema inference.

\**\* [Avro logical types](https://avro.apache.org/docs/current/spec.html#Logical+Types)

Unsupported Avro logical data types:
- `time-millis`
- `time-micros`
- `duration`