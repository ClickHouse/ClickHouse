---
title: مطابقة أنواع البيانات
---

يوضح الجدول أدناه جميع أنواع البيانات التي يدعمها تنسيق Apache Avro، وأنواع بيانات ClickHouse [المقابلة](/ar/sql-reference/data-types/index.md) لها في استعلامات `INSERT` و`SELECT`.

| نوع بيانات Avro `INSERT`                    | نوع بيانات ClickHouse                                                                                         | نوع بيانات Avro `SELECT`      |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| `boolean`, `int`, `long`, `float`, `double` | [Int(8\16\32)](/ar/sql-reference/data-types/int-uint.md), [UInt(8\16\32)](/ar/sql-reference/data-types/int-uint.md) | `int`                         |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](/ar/sql-reference/data-types/int-uint.md), [UInt64](/ar/sql-reference/data-types/int-uint.md)               | `long`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](/ar/sql-reference/data-types/float.md)                                                                 | `float`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float64](/ar/sql-reference/data-types/float.md)                                                                 | `double`                      |
| `bytes`, `string`, `fixed`, `enum`          | [String](/ar/sql-reference/data-types/string.md)                                                                 | `bytes` أو `string` *         |
| `bytes`, `string`, `fixed`                  | [FixedString(N)](/ar/sql-reference/data-types/fixedstring.md)                                                    | `fixed(N)`                    |
| `enum`                                      | [Enum(8\16)](/ar/sql-reference/data-types/enum.md)                                                               | `enum`                        |
| `array(T)`                                  | [Array(T)](/ar/sql-reference/data-types/array.md)                                                                | `array(T)`                    |
| `map(V, K)`                                 | [Map(V, K)](/ar/sql-reference/data-types/map.md)                                                                 | `map(string, K)`              |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](/ar/sql-reference/data-types/date.md)                                                              | `union(null, T)`              |
| `union(T1, T2, …)` **                       | [Variant(T1, T2, …)](/ar/sql-reference/data-types/variant.md)                                                    | `union(T1, T2, …)` **         |
| `null`                                      | [Nullable(Nothing)](/ar/sql-reference/data-types/special-data-types/nothing.md)                                  | `null`                        |
| `int (date)` ***                            | [Date](/ar/sql-reference/data-types/date.md), [Date32](/ar/sql-reference/data-types/date32.md)                      | `int (date)` ***              |
| `long (timestamp-millis)` ***               | [DateTime64(3)](/ar/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-millis)` *** |
| `long (timestamp-micros)` ***               | [DateTime64(6)](/ar/sql-reference/data-types/datetime.md)                                                        | `long (timestamp-micros)` *** |
| `bytes (decimal)`  ***                      | [DateTime64(N)](/ar/sql-reference/data-types/datetime.md)                                                        | `bytes (decimal)`  ***        |
| `int`                                       | [IPv4](/ar/sql-reference/data-types/ipv4.md)                                                                     | `int`                         |
| `fixed(16)`                                 | [IPv6](/ar/sql-reference/data-types/ipv6.md)                                                                     | `fixed(16)`                   |
| `bytes (decimal)` ***                       | [Decimal(P, S)](/ar/sql-reference/data-types/decimal.md)                                                         | `bytes (decimal)` ***         |
| `string (uuid)` ***                         | [UUID](/ar/sql-reference/data-types/uuid.md)                                                                     | `string (uuid)` ***           |
| `fixed(16)`                                 | [Int128/UInt128](/ar/sql-reference/data-types/int-uint.md)                                                       | `fixed(16)`                   |
| `fixed(32)`                                 | [Int256/UInt256](/ar/sql-reference/data-types/int-uint.md)                                                       | `fixed(32)`                   |
| `record`                                    | [Tuple](/ar/sql-reference/data-types/tuple.md)                                                                   | `record`                      |

* القيمة الافتراضية هي `bytes`، ويتحكم بهذا الإعداد [`output_format_avro_string_column_pattern`](/ar/operations/settings/settings-formats.md/#output_format_avro_string_column_pattern)

**  يقبل [نوع Variant](/ar/sql-reference/data-types/variant) القيمة `null` ضمنيًا كقيمة للحقل، لذا فإن Avro `union(T1, T2, null)` سيُحوَّل، على سبيل المثال، إلى `Variant(T1, T2)`.
ونتيجةً لذلك، عند إنشاء Avro من ClickHouse، يجب علينا دائمًا تضمين النوع `null` ضمن مجموعة أنواع Avro `union` لأننا لا نعرف أثناء استدلال المخطط ما إذا كانت أي قيمة تساوي بالفعل `null`.

*** [الأنواع المنطقية في Avro](https://avro.apache.org/docs/current/spec.html#Logical+Types)

أنواع البيانات المنطقية غير المدعومة في Avro:

* `time-millis`
* `time-micros`
* `duration`