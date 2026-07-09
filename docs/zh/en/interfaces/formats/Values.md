---
alias: []
description: 'Values 格式文档'
input_format: true
keywords: ['Values']
output_format: true
slug: /interfaces/formats/Values
title: 'Values'
doc_type: 'guide'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 描述
</div>

`Values` 格式会将每一行打印在括号中。

* 各行之间用逗号分隔，最后一行后不加逗号。
* 括号内的值也以逗号分隔。
* 数值以十进制格式输出，不带引号。
* 数组输出为 `[]`。
* String、Date 以及带时间的日期值会加引号输出。
* 转义规则和解析方式与 [TabSeparated](TabSeparated/TabSeparated.md) 格式类似。

格式化时不会插入额外空格；但解析时允许这些空格，并会将其跳过 (数组值内的空格除外，不允许出现) 。
[`NULL`](/zh/sql-reference/syntax.md) 表示为 `NULL`。

以 `Values` 格式传递数据时，至少需要转义以下字符：

* 单引号
* 反斜杠

这是 `INSERT INTO t VALUES ...` 使用的格式，但也可以用于格式化查询结果。

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

`Values` 格式是 `INSERT` 使用的格式，因此任何 `INSERT ... VALUES` 语句
实际上都已经在使用它。也可以显式指定 `FORMAT Values` 子句，并且这些
行可以通过 stream 或文件提供。每一行都是一个用括号括起来、
以逗号分隔的 Tuple，而各个 Tuple 之间也用逗号分隔：

```sql title="Query"
CREATE TABLE t (id UInt32, name String, values Array(UInt32)) ENGINE = Memory;

INSERT INTO t FORMAT Values (1, 'a', [10, 20]), (2, 'b', [30]);

SELECT * FROM t ORDER BY id;
```

```response title="Response"
┌─id─┬─name─┬─values──┐
│  1 │ a    │ [10,20] │
│  2 │ b    │ [30]    │
└────┴──────┴─────────┘
```

<div id="using-expressions">
  ### 在输入中使用表达式
</div>

与大多数输入格式不同，`Values` 可以对每个字段中的 SQL 表达式求值，
而不只接受字面量。这由
[`input_format_values_interpret_expressions`](#format-settings) 控制 (默认
启用) ：当某个字段无法被快速流式解析器读取时，ClickHouse
会回退到 SQL 解析器，并将该字段按表达式进行解析。

```sql title="Query"
CREATE TABLE prices (item String, total UInt32) ENGINE = Memory;

INSERT INTO prices FORMAT Values ('apple', 3 * 4), ('pear', length('hello') + 10);

SELECT * FROM prices ORDER BY total;
```

```response title="Response"
┌─item──┬─total─┐
│ apple │    12 │
│ pear  │    15 │
└───────┴───────┘
```

<div id="selecting-data">
  ### 选择数据
</div>

Values 格式也可用于格式化查询结果。数值
书写时不加引号，数组使用 `[]` 表示，字符串和日期使用单引号；
字符串中的单引号和反斜杠会用反斜杠转义，而
[`NULL`](/zh/sql-reference/syntax.md) 则写作 `NULL`：

```sql title="Query"
SELECT 1 AS a, 'O''Reilly' AS b, NULL::Nullable(String) AS c FORMAT Values;
```

```response title="Response"
(1,'O\'Reilly',NULL)
```

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                          | 说明                                                                      | 默认值    |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | ------ |
| [`input_format_values_interpret_expressions`](../../operations/settings/settings-formats.md/#input_format_values_interpret_expressions)                     | 如果字段无法被流式解析器解析，则运行 SQL 解析器，并尝试将其解释为 SQL 表达式。                            | `true` |
| [`input_format_values_deduce_templates_of_expressions`](../../operations/settings/settings-formats.md/#input_format_values_deduce_templates_of_expressions) | 如果字段无法被流式解析器解析，则运行 SQL 解析器，推导 SQL 表达式的模板，尝试使用该模板解析所有行，然后将其作为表达式解释到所有行中。 | `true` |
| [`input_format_values_accurate_types_of_literals`](../../operations/settings/settings-formats.md/#input_format_values_accurate_types_of_literals)           | 在使用模板解析并解释表达式时，检查字面量的实际类型，以避免可能出现的溢出和精度问题。                              | `true` |