---
alias: []
description: 'BSONEachRow 格式的文档'
input_format: true
keywords: ['BSONEachRow']
output_format: true
slug: /interfaces/formats/BSONEachRow
title: 'BSONEachRow'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

`BSONEachRow` 格式会将数据解析为一系列连续的二进制 JSON (BSON) 文档，文档之间不使用任何分隔符。
每一行都会被格式化为一个文档，每一列都会被格式化为 BSON 文档中的一个字段，并以列名作为键。

<div id="data-types-matching">
  ## 数据类型匹配
</div>

输出时，使用以下 ClickHouse 类型与 BSON 类型之间的对应关系：

| ClickHouse 类型                                                                                         | BSON 类型                                                                                                |
| ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| [Bool](/zh/sql-reference/data-types/boolean.md)                                                          | `\x08` 布尔值                                                                                             |
| [Int8/UInt8](/zh/sql-reference/data-types/int-uint.md)/[Enum8](/zh/sql-reference/data-types/enum.md)        | `\x10` int32                                                                                           |
| [Int16/UInt16](/zh/sql-reference/data-types/int-uint.md)/[Enum16](/zh/sql-reference/data-types/enum.md)     | `\x10` int32                                                                                           |
| [Int32](/zh/sql-reference/data-types/int-uint.md)                                                        | `\x10` int32                                                                                           |
| [UInt32](/zh/sql-reference/data-types/int-uint.md)                                                       | `\x12` int64                                                                                           |
| [Int64/UInt64](/zh/sql-reference/data-types/int-uint.md)                                                 | `\x12` int64                                                                                           |
| [Float32/Float64](/zh/sql-reference/data-types/float.md)                                                 | `\x01` double                                                                                          |
| [Date](/zh/sql-reference/data-types/date.md)/[Date32](/zh/sql-reference/data-types/date32.md)               | `\x10` int32                                                                                           |
| [DateTime](/zh/sql-reference/data-types/datetime.md)                                                     | `\x12` int64                                                                                           |
| [DateTime64](/zh/sql-reference/data-types/datetime64.md)                                                 | `\x09` 日期时间                                                                                        |
| [Decimal32](/zh/sql-reference/data-types/decimal.md)                                                     | `\x10` int32                                                                                           |
| [Decimal64](/zh/sql-reference/data-types/decimal.md)                                                     | `\x12` int64                                                                                           |
| [Decimal128](/zh/sql-reference/data-types/decimal.md)                                                    | `\x05` 二进制，`\x00` 二进制子类型，大小 = 16                                                                       |
| [Decimal256](/zh/sql-reference/data-types/decimal.md)                                                    | `\x05` 二进制，`\x00` 二进制子类型，大小 = 32                                                                       |
| [Int128/UInt128](/zh/sql-reference/data-types/int-uint.md)                                               | `\x05` 二进制，`\x00` 二进制子类型，大小 = 16                                                                       |
| [Int256/UInt256](/zh/sql-reference/data-types/int-uint.md)                                               | `\x05` 二进制，`\x00` 二进制子类型，大小 = 32                                                                       |
| [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md) | `\x05` 二进制，`\x00` 二进制子类型；如果启用了设置 output&#95;format&#95;bson&#95;string&#95;as&#95;string，则为 `\x02` 字符串 |
| [UUID](/zh/sql-reference/data-types/uuid.md)                                                             | `\x05` 二进制，`\x04` UUID 子类型，大小 = 16                                                                     |
| [Array](/zh/sql-reference/data-types/array.md)                                                           | `\x04` 数组                                                                                              |
| [Tuple](/zh/sql-reference/data-types/tuple.md)                                                           | `\x04` 数组                                                                                              |
| [命名元组](/zh/sql-reference/data-types/tuple.md)                                                            | `\x03` 文档                                                                                              |
| [Map](/zh/sql-reference/data-types/map.md)                                                               | `\x03` 文档                                                                                              |
| [IPv4](/zh/sql-reference/data-types/ipv4.md)                                                             | `\x10` int32                                                                                           |
| [IPv6](/zh/sql-reference/data-types/ipv6.md)                                                             | `\x05` 二进制，`\x00` 二进制子类型                                                                               |

输入时，使用以下 BSON 类型与 ClickHouse 类型之间的对应关系：

| BSON 类型                      | ClickHouse 类型                                                                                                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `\x01` 双精度浮点数                | [Float32/Float64](/zh/sql-reference/data-types/float.md)                                                                                                                                               |
| `\x02` 字符串                   | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x03` 文档                    | [Map](/zh/sql-reference/data-types/map.md)/[命名元组](/zh/sql-reference/data-types/tuple.md)                                                                                                                  |
| `\x04` 数组                    | [Array](/zh/sql-reference/data-types/array.md)/[Tuple](/zh/sql-reference/data-types/tuple.md)                                                                                                             |
| `\x05` 二进制，`\x00` 二进制子类型     | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)/[IPv6](/zh/sql-reference/data-types/ipv6.md)                                                     |
| `\x05` 二进制，`\x02` 旧二进制子类型    | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x05` 二进制，`\x03` 旧 UUID 子类型 | [UUID](/zh/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x05` 二进制，`\x04` UUID 子类型   | [UUID](/zh/sql-reference/data-types/uuid.md)                                                                                                                                                           |
| `\x07` ObjectId              | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x08` 布尔值                   | [Bool](/zh/sql-reference/data-types/boolean.md)                                                                                                                                                        |
| `\x09` 日期时间                  | [DateTime64](/zh/sql-reference/data-types/datetime64.md)                                                                                                                                               |
| `\x0A` NULL 值                | [NULL](/zh/sql-reference/data-types/nullable.md)                                                                                                                                                       |
| `\x0D` JavaScript 代码         | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x0E` 符号                    | [String](/zh/sql-reference/data-types/string.md)/[FixedString](/zh/sql-reference/data-types/fixedstring.md)                                                                                               |
| `\x10` int32                 | [Int32/UInt32](/zh/sql-reference/data-types/int-uint.md)/[Decimal32](/zh/sql-reference/data-types/decimal.md)/[IPv4](/zh/sql-reference/data-types/ipv4.md)/[Enum8/Enum16](/zh/sql-reference/data-types/enum.md) |
| `\x12` int64                 | [Int64/UInt64](/zh/sql-reference/data-types/int-uint.md)/[Decimal64](/zh/sql-reference/data-types/decimal.md)/[DateTime64](/zh/sql-reference/data-types/datetime64.md)                                       |

不支持其他 BSON 类型。此外，它还支持不同整数类型之间的转换。
例如，可以将 BSON `int32` 值以 [`UInt8`](../../sql-reference/data-types/int-uint.md) 的形式插入 ClickHouse。

`Int128`/`UInt128`/`Int256`/`UInt256`/`Decimal128`/`Decimal256` 等大整数和 Decimal 类型，可以从二进制子类型为 `\x00` 的 BSON Binary 值中解析。
在这种情况下，该格式会验证二进制数据的大小是否与预期值的大小一致。

:::note
此格式在大端序平台上无法正常工作。
:::

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用一个名为 `football.bson` 的 BSON 文件，其中包含以下数据：

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.bson' FORMAT BSONEachRow;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `BSONEachRow` 格式读取数据：

```sql
SELECT *
FROM football INTO OUTFILE 'docs_data/bson/football.bson'
FORMAT BSONEachRow
```

:::tip
BSON 是一种二进制格式，无法在终端中以人类可读的形式显示。请使用 `INTO OUTFILE` 将输出写入 BSON 文件。
:::

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                                                                    | 描述                                           | 默认值     |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------- | ------- |
| [`output_format_bson_string_as_string`](../../operations/settings/settings-formats.md/#output_format_bson_string_as_string)                                                                           | 对 String 列使用 BSON String 类型，而不是 Binary 类型。   | `false` |
| [`input_format_bson_skip_fields_with_unsupported_types_in_schema_inference`](../../operations/settings/settings-formats.md/#input_format_bson_skip_fields_with_unsupported_types_in_schema_inference) | 在对 BSONEachRow 格式执行 schema 推断时，允许跳过类型不受支持的列。 | `false` |