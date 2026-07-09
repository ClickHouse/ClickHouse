---
alias: []
description: 'ORC 格式文档'
input_format: true
keywords: ['ORC']
output_format: true
slug: /interfaces/formats/ORC
title: 'ORC'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

[Apache ORC](https://orc.apache.org/) 是一种在 [Hadoop](https://hadoop.apache.org/) 生态中广泛使用的列式存储格式。

<div id="data-types-matching-orc">
  ## 数据类型匹配
</div>

下表比较了受支持的 ORC 数据类型，以及它们在 `INSERT` 和 `SELECT` 查询中对应的 ClickHouse [数据类型](/zh/sql-reference/data-types/index.md)。

| ORC 数据类型 (`INSERT`)                   | ClickHouse 数据类型                                                                                   | ORC 数据类型 (`SELECT`) |
| ------------------------------------- | ------------------------------------------------------------------------------------------------- | ------------------- |
| `Boolean`                             | [UInt8](/zh/sql-reference/data-types/int-uint.md)                                                    | `Boolean`           |
| `Tinyint`                             | [Int8/UInt8](/zh/sql-reference/data-types/int-uint.md)/[Enum8](/zh/sql-reference/data-types/enum.md)    | `Tinyint`           |
| `Smallint`                            | [Int16/UInt16](/zh/sql-reference/data-types/int-uint.md)/[Enum16](/zh/sql-reference/data-types/enum.md) | `Smallint`          |
| `Int`                                 | [Int32/UInt32](/zh/sql-reference/data-types/int-uint.md)                                             | `Int`               |
| `Bigint`                              | [Int64/UInt32](/zh/sql-reference/data-types/int-uint.md)                                             | `Bigint`            |
| `Float`                               | [Float32](/zh/sql-reference/data-types/float.md)                                                     | `Float`             |
| `Double`                              | [Float64](/zh/sql-reference/data-types/float.md)                                                     | `Double`            |
| `Decimal`                             | [Decimal](/zh/sql-reference/data-types/decimal.md)                                                   | `Decimal`           |
| `Date`                                | [Date32](/zh/sql-reference/data-types/date32.md)                                                     | `Date`              |
| `Timestamp`                           | [DateTime64](/zh/sql-reference/data-types/datetime64.md)                                             | `Timestamp`         |
| `String`, `Char`, `Varchar`, `Binary` | [String](/zh/sql-reference/data-types/string.md)                                                     | `Binary`            |
| `List`                                | [Array](/zh/sql-reference/data-types/array.md)                                                       | `List`              |
| `Struct`                              | [Tuple](/zh/sql-reference/data-types/tuple.md)                                                       | `Struct`            |
| `Map`                                 | [Map](/zh/sql-reference/data-types/map.md)                                                           | `Map`               |
| `Int`                                 | [IPv4](/zh/sql-reference/data-types/int-uint.md)                                                     | `Int`               |
| `Binary`                              | [IPv6](/zh/sql-reference/data-types/ipv6.md)                                                         | `Binary`            |
| `Binary`                              | [Int128/UInt128/Int256/UInt256](/zh/sql-reference/data-types/int-uint.md)                            | `Binary`            |
| `Binary`                              | [Decimal256](/zh/sql-reference/data-types/decimal.md)                                                | `Binary`            |

* 不支持其他类型。
* Array 可以嵌套，且其参数值可以为 `Nullable` 类型。`Tuple` 和 `Map` 类型也可以嵌套。
* ClickHouse 表中列的数据类型不必与对应的 ORC 数据字段类型一致。插入数据时，ClickHouse 会根据上表解析数据类型，然后将数据[转换](/zh/sql-reference/functions/type-conversion-functions#CAST)为 ClickHouse 表列设置的数据类型。

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用名为 `football.orc`、包含以下数据的 ORC 文件：

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
INSERT INTO football FROM INFILE 'football.orc' FORMAT ORC;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `ORC` 格式读取数据：

```sql
SELECT *
FROM football
INTO OUTFILE 'football.orc'
FORMAT ORC
```

:::tip
ORC 是一种二进制格式，无法在终端中以人类可读的方式显示。请使用 `INTO OUTFILE` 输出 ORC 文件。
:::

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                                                                   | 说明                                         | 默认值     |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | ------- |
| [`output_format_arrow_string_as_string`](/zh/operations/settings/settings-formats.md/#output_format_arrow_string_as_string)                                                                             | 对 String 类型列使用 Arrow String 类型，而不是 Binary。 | `false` |
| [`output_format_orc_compression_method`](/zh/operations/settings/settings-formats.md/#output_format_orc_compression_method)                                                                             | 输出 ORC 格式使用的压缩方法。默认值                       | `none`  |
| [`input_format_arrow_case_insensitive_column_matching`](/zh/operations/settings/settings-formats.md/#input_format_arrow_case_insensitive_column_matching)                                               | 匹配 Arrow 列与 ClickHouse 列时忽略大小写。            | `false` |
| [`input_format_arrow_allow_missing_columns`](/zh/operations/settings/settings-formats.md/#input_format_arrow_allow_missing_columns)                                                                     | 读取 Arrow 数据时允许缺失列。                         | `false` |
| [`input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference`](/zh/operations/settings/settings-formats.md/#input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference) | 对 Arrow 格式进行 schema 推断时，允许跳过类型不受支持的列。      | `false` |

要与 Hadoop 交换数据，可以使用 [HDFS 表引擎](/zh/engines/table-engines/integrations/hdfs.md)。