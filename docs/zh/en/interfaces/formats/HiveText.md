---
alias: []
description: 'HiveText 格式文档'
input_format: true
keywords: ['HiveText']
output_format: false
slug: /interfaces/formats/HiveText
title: 'HiveText'
doc_type: '参考'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 描述
</div>

`HiveText` 读取 [Apache Hive](https://hive.apache.org/) 表使用的文本序列化格式 (即由 Hive 的 `LazySimpleSerDe` 生成的格式) 。它是一种带分隔符的文本格式，类似于 [`CSV`](/zh/interfaces/formats/CSV)，其中各字段由 Hive 默认的 `\x01` (Ctrl-A) 分隔符分隔。字段分隔符可通过 [`input_format_hive_text_fields_delimiter`](#format-settings) 配置。

`HiveText` 是一种仅支持输入的格式。数据没有表头：值会按位置映射到目标表的列，因此列名和类型取自该表 (或显式提供的结构) ，而不是从数据中自动推断。读取时，ClickHouse 会以尽力模式解析日期和时间 (参见 [`date_time_input_format`](/zh/operations/settings/formats#date_time_input_format)) ，用列默认值填充末尾缺失的字段，并跳过无法识别的字段。

在单个字段内，值使用与 `CSV` 相同的转义规则进行解析，而不是使用 Hive 的嵌套分隔符。特别是，类型为 [`Array`](/zh/sql-reference/data-types/array) 的列会从带方括号的表示形式中读取 (例如 `"['a','b','c']"`) ，而不是从由 Hive 集合分隔符 `\x02` 分隔的值中读取。

:::note 嵌套分隔符设置不起作用
[`input_format_hive_text_collection_items_delimiter`](#format-settings) 和
[`input_format_hive_text_map_keys_delimiter`](#format-settings) 设置出于兼容性会被接受，但当前解析时并不会使用。
:::

默认情况下，允许各行包含数量可变的字段 (参见
[`input_format_hive_text_allow_variable_number_of_columns`](#format-settings)) ：字段数少于表列数的行会用默认值填充缺失列，而末尾带有额外字段的行则会跳过这些多余字段。

<div id="example-usage">
  ## 示例用法
</div>

下面的示例使用
[`input_format_hive_text_fields_delimiter`](#format-settings) 将默认字段分隔符改为逗号 (`,`) ，以便于阅读输入
文件。

<div id="reading-data">
  ### 读取 HiveText 文件
</div>

给定一个以逗号分隔字段的文件 `hive_data.txt`：

```text title="hive_data.txt"
1,3
3,5,9
```

我们创建一个表来定义列名和类型，并使用 `FORMAT HiveText` 将文件插入该表中：

```sql title="Query"
CREATE TABLE test_tbl (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_tbl FROM INFILE 'hive_data.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_tbl;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 3 │ 0 │
│ 3 │ 5 │ 9 │
└───┴───┴───┘
```

请注意，第一行 `1,3` 只有两个字段，因此缺失的列 `c`
会被填入默认值 `0`。

<div id="variable-number-of-columns">
  ### 可变列数
</div>

在默认设置 `input_format_hive_text_allow_variable_number_of_columns = 1` 下，
如果某行的字段数超过表中的列数，末尾多出的字段会被直接跳过：

```text title="hive_extras.txt"
1,2,3,4,5
6,7,8
```

```sql title="Query"
CREATE TABLE test_extras (a UInt16, b UInt32, c UInt32) ENGINE = MergeTree ORDER BY a;

INSERT INTO test_extras FROM INFILE 'hive_extras.txt'
SETTINGS input_format_hive_text_fields_delimiter = ','
FORMAT HiveText;

SELECT * FROM test_extras ORDER BY a;
```

```response title="Response"
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 6 │ 7 │ 8 │
└───┴───┴───┘
```

如果将 `input_format_hive_text_allow_variable_number_of_columns = 0`，
则会强制要求字段数严格一致，字段数少于表字段数的行会引发
解析异常。

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                        | 说明                                                           | 默认值    |
| --------------------------------------------------------- | ------------------------------------------------------------ | ------ |
| `input_format_hive_text_fields_delimiter`                 | Hive Text File 中字段之间的分隔符                                     | `\x01` |
| `input_format_hive_text_collection_items_delimiter`       | Hive Text File 中集合 (Array 或 Map) 项之间的分隔符。此设置可被接受，但当前解析时不会使用。 | `\x02` |
| `input_format_hive_text_map_keys_delimiter`               | Hive Text File 中一组 Map 键/值之间的分隔符。此设置可被接受，但当前解析时不会使用。         | `\x03` |
| `input_format_hive_text_allow_variable_number_of_columns` | 忽略 Hive Text 输入中的额外列 (如果文件中的列数多于预期) ，并将缺失字段视为默认值             | `1`    |