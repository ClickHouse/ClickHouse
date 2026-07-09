---
alias: []
description: 'CSV 格式文档'
input_format: true
keywords: ['CSV']
output_format: true
slug: /interfaces/formats/CSV
title: 'CSV'
doc_type: 'reference'
---

<div id="description">
  ## 描述
</div>

逗号分隔值格式 ([RFC](https://tools.ietf.org/html/rfc4180)) 。
进行格式化时，行会用双引号括起来。字符串内部的双引号会输出为两个连续的双引号。
除此之外，没有其他字符转义规则。

* Date 和 date-time 会用双引号括起来。
* 数字输出时不带引号。
* 各个值之间用分隔符字符分隔，默认为 `,`。分隔符字符由设置 [format&#95;csv&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_csv_delimiter) 定义。
* 各行之间使用 Unix 换行符 (LF) 分隔。
* 数组在 CSV 中按如下方式序列化：
  * 首先，数组会像在 TabSeparated 格式中那样被序列化为字符串
  * 然后将得到的字符串用双引号括起来输出到 CSV。
* CSV 格式中的 Tuples 会被序列化为单独的列 (也就是说，它们在 Tuple 中的嵌套结构会丢失) 。

```bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

:::note
默认情况下，分隔符为 `,`
更多信息请参见设置 [format&#95;csv&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_csv_delimiter)。
:::

解析时，所有值都可以带引号或不带引号。支持双引号和单引号。

行也可以不加引号。在这种情况下，会一直解析到分隔符字符或换行符 (CR 或 LF) 为止。
不过，尽管这不符合 RFC 规范，在解析不带引号的行时，仍会忽略开头和末尾的空格与制表符。
支持的换行类型包括：Unix (LF) 、Windows (CR LF) 和 Mac OS Classic (CR LF) 。

`NULL` 的格式由设置 [format&#95;csv&#95;null&#95;representation](/zh/operations/settings/settings-formats.md/#format_csv_null_representation) 决定 (默认值为 `\N`) 。

在输入数据中，`ENUM` 值可以表示为名称或 ID。
首先，我们会尝试将输入值与 `ENUM` 名称匹配。
如果失败，并且输入值是数值，则会尝试将该数值与 `ENUM` ID 匹配。
如果输入数据仅包含 `ENUM` ID，建议启用设置 [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/zh/operations/settings/settings-formats.md/#input_format_csv_enum_as_number) 以优化 `ENUM` 解析。

<div id="example-usage">
  ## 使用示例
</div>

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                                                       | 描述                                                        | 默认值     | 备注                                                                                                                                                                   |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [format&#95;csv&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_csv_delimiter)                                                                                           | 在 CSV 数据中用作分隔符的字符。                                        | `,`     |                                                                                                                                                                      |
| [format&#95;csv&#95;allow&#95;single&#95;quotes](/zh/operations/settings/settings-formats.md/#format_csv_allow_single_quotes)                                                               | 允许使用单引号括起来的字符串。                                           | `true`  |                                                                                                                                                                      |
| [format&#95;csv&#95;allow&#95;double&#95;quotes](/zh/operations/settings/settings-formats.md/#format_csv_allow_double_quotes)                                                               | 允许使用双引号括起来的字符串。                                           | `true`  |                                                                                                                                                                      |
| [format&#95;csv&#95;null&#95;representation](/zh/operations/settings/settings-formats.md/#format_tsv_null_representation)                                                                   | CSV 格式中的自定义 NULL 表示形式。                                    | `\N`    |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;empty&#95;as&#95;default](/zh/operations/settings/settings-formats.md/#input_format_csv_empty_as_default)                                                     | 将 CSV 输入中的空字段视为默认值。                                       | `true`  | 对于复杂的默认表达式，还必须启用 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/zh/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)。 |
| [input&#95;format&#95;csv&#95;enum&#95;as&#95;number](/zh/operations/settings/settings-formats.md/#input_format_csv_enum_as_number)                                                         | 将 CSV 格式中插入的枚举值视为枚举索引。                                    | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;use&#95;best&#95;effort&#95;in&#95;schema&#95;inference](/zh/operations/settings/settings-formats.md/#input_format_csv_use_best_effort_in_schema_inference)   | 在 CSV 格式中使用一些调整和启发式方法进行 schema 推断。如果禁用，所有字段都将被推断为 String。 | `true`  |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;arrays&#95;as&#95;nested&#95;csv](/zh/operations/settings/settings-formats.md/#input_format_csv_arrays_as_nested_csv)                                         | 从 CSV 读取 Array 时，要求其元素先以嵌套 CSV 的方式序列化，再放入字符串中。            | `false` |                                                                                                                                                                      |
| [output&#95;format&#95;csv&#95;crlf&#95;end&#95;of&#95;line](/zh/operations/settings/settings-formats.md/#output_format_csv_crlf_end_of_line)                                               | 如果设置为 true，CSV 输出格式的行尾将使用 `\r\n` 而不是 `\n`。                | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;skip&#95;first&#95;lines](/zh/operations/settings/settings-formats.md/#input_format_csv_skip_first_lines)                                                     | 跳过数据开头指定数量的行。                                             | `0`     |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;detect&#95;header](/zh/operations/settings/settings-formats.md/#input_format_csv_detect_header)                                                               | 自动检测 CSV 格式中包含名称和类型的表头。                                   | `true`  |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;skip&#95;trailing&#95;empty&#95;lines](/zh/operations/settings/settings-formats.md/#input_format_csv_skip_trailing_empty_lines)                               | 跳过数据末尾的空行。                                                | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;trim&#95;whitespaces](/zh/operations/settings/settings-formats.md/#input_format_csv_trim_whitespaces)                                                         | 去除未加引号的 CSV 字符串中的空格和制表符。                                  | `true`  |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;allow&#95;whitespace&#95;or&#95;tab&#95;as&#95;delimiter](/zh/operations/settings/settings-formats.md/#input_format_csv_allow_whitespace_or_tab_as_delimiter) | 允许在 CSV 字符串中使用空白字符或制表符作为字段分隔符。                            | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;allow&#95;variable&#95;number&#95;of&#95;columns](/zh/operations/settings/settings-formats.md/#input_format_csv_allow_variable_number_of_columns)             | 允许 CSV 格式中的列数可变，忽略多余的列，并为缺失的列使用默认值。                       | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;use&#95;default&#95;on&#95;bad&#95;values](/zh/operations/settings/settings-formats.md/#input_format_csv_use_default_on_bad_values)                           | 当 CSV 字段因错误值导致反序列化失败时，允许为该列设置默认值。                         | `false` |                                                                                                                                                                      |
| [input&#95;format&#95;csv&#95;try&#95;infer&#95;numbers&#95;from&#95;strings](/zh/operations/settings/settings-formats.md/#input_format_csv_try_infer_numbers_from_strings)                 | 在 schema 推断期间，尝试从字符串字段中推断出数值。                             | `false` |                                                                                                                                                                      |