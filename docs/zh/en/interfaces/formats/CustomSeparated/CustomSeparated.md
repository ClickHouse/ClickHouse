---
alias: []
description: 'CustomSeparated 格式文档'
input_format: true
keywords: ['CustomSeparated']
output_format: true
slug: /interfaces/formats/CustomSeparated
title: 'CustomSeparated'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

与 [Template](../Template/Template.md) 类似，但它会输出或读取所有列的名称和类型，并使用 [format&#95;custom&#95;escaping&#95;rule](../../../operations/settings/settings-formats.md/#format_custom_escaping_rule) 设置中的转义规则，以及以下设置中的分隔符：

* [format&#95;custom&#95;field&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_field_delimiter)
* [format&#95;custom&#95;row&#95;before&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_row_before_delimiter)
* [format&#95;custom&#95;row&#95;after&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_row_after_delimiter)
* [format&#95;custom&#95;row&#95;between&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_row_between_delimiter)
* [format&#95;custom&#95;result&#95;before&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_result_before_delimiter)
* [format&#95;custom&#95;result&#95;after&#95;delimiter](/zh/operations/settings/settings-formats.md/#format_custom_result_after_delimiter)

:::note
它不会使用格式字符串中的转义规则设置和分隔符。
:::

此外，还有 [`CustomSeparatedIgnoreSpaces`](../CustomSeparated/CustomSeparatedIgnoreSpaces.md) 格式，它与 [TemplateIgnoreSpaces](../Template//TemplateIgnoreSpaces.md) 类似。

<div id="example-usage">
  ## 示例用法
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用以下名为 `football.txt` 的 txt 文件：

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

配置自定义分隔符设置：

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.txt' FORMAT CustomSeparated;
```

<div id="reading-data">
  ### 读取数据
</div>

配置自定义分隔符设置：

```sql
SET format_custom_row_before_delimiter = 'row(';
SET format_custom_row_after_delimiter = ')';
SET format_custom_field_delimiter = ';';
SET format_custom_row_between_delimiter = ',';
SET format_custom_escaping_rule = 'Quoted';
```

使用 `CustomSeparated` 格式读取数据：

```sql
SELECT *
FROM football
FORMAT CustomSeparated
```

输出将采用已配置的自定义格式：

```text
row('2022-04-30';2021;'Sutton United';'Bradford City';1;4),row('2022-04-30';2021;'Swindon Town';'Barrow';2;1),row('2022-04-30';2021;'Tranmere Rovers';'Oldham Athletic';2;0),row('2022-05-02';2021;'Port Vale';'Newport County';1;2),row('2022-05-02';2021;'Salford City';'Mansfield Town';2;2),row('2022-05-07';2021;'Barrow';'Northampton Town';1;3),row('2022-05-07';2021;'Bradford City';'Carlisle United';2;0),row('2022-05-07';2021;'Bristol Rovers';'Scunthorpe United';7;0),row('2022-05-07';2021;'Exeter City';'Port Vale';0;1),row('2022-05-07';2021;'Harrogate Town A.F.C.';'Sutton United';0;2),row('2022-05-07';2021;'Hartlepool United';'Colchester United';0;2),row('2022-05-07';2021;'Leyton Orient';'Tranmere Rovers';0;1),row('2022-05-07';2021;'Mansfield Town';'Forest Green Rovers';2;2),row('2022-05-07';2021;'Newport County';'Rochdale';0;2),row('2022-05-07';2021;'Oldham Athletic';'Crawley Town';3;3),row('2022-05-07';2021;'Stevenage Borough';'Salford City';4;2),row('2022-05-07';2021;'Walsall';'Swindon Town';0;3)
```

<div id="format-settings">
  ## 格式设置
</div>

附加设置：

| 设置                                                                                                                                                                                         | 说明                                              | 默认值     |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------- | ------- |
| [input&#95;format&#95;custom&#95;detect&#95;header](../../../operations/settings/settings-formats.md/#input_format_custom_detect_header)                                                   | 如果存在包含名称和类型的头部，则启用自动检测。                         | `true`  |
| [input&#95;format&#95;custom&#95;skip&#95;trailing&#95;empty&#95;lines](../../../operations/settings/settings-formats.md/#input_format_custom_skip_trailing_empty_lines)                   | 跳过文件末尾的空行。                                      | `false` |
| [input&#95;format&#95;custom&#95;allow&#95;variable&#95;number&#95;of&#95;columns](../../../operations/settings/settings-formats.md/#input_format_custom_allow_variable_number_of_columns) | 允许 CustomSeparated 格式中的列数可变，忽略多余的列，并对缺失的列使用默认值。 | `false` |