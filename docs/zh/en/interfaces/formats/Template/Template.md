---
alias: []
description: 'Template 格式文档'
input_format: true
keywords: ['Template']
output_format: true
slug: /interfaces/formats/Template
title: 'Template'
doc_type: 'guide'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

对于需要比其他标准格式提供更高自定义程度的场景，
`Template` 格式允许用户使用带有值占位符的自定义格式字符串，
并为数据指定转义规则。

它使用以下设置：

| 设置                                                                                  | 说明                                                         |
| ----------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| [`format_template_row`](#format_template_row)                                       | 指定包含行格式字符串的文件路径。                                           |
| [`format_template_resultset`](#format_template_resultset)                           | 指定包含行格式字符串的文件路径                                            |
| [`format_template_rows_between_delimiter`](#format_template_rows_between_delimiter) | 指定行与行之间的分隔符，该分隔符会在每一行之后输出 (最后一行除外) ，或在读取时被期望出现 (默认为 `\n`)  |
| `format_template_row_format`                                                        | [内联](#inline_specification)指定行的格式字符串。                      |
| `format_template_resultset_format`                                                  | [内联](#inline_specification)指定结果集的格式字符串。                    |
| 其他格式的一些设置 (例如，使用 `JSON` 转义时的 `output_format_json_quote_64bit_integers`              |                                                            |

<div id="settings-and-escaping-rules">
  ## 设置与转义规则
</div>

<div id="format_template_row">
  ### format_template_row
</div>

设置 `format_template_row` 用于指定包含行格式字符串的文件路径，其语法如下：

```text
delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N
```

其中：

| 语法部分            | 说明                              |
| --------------- | ------------------------------- |
| `delimiter_i`   | 值之间的分隔符 (`$` 符号可转义为 `$$`)       |
| `column_i`      | 需要选取或插入其值的列名或列索引 (如果为空，则会跳过该列)  |
| `serializeAs_i` | 列值的转义规则。                        |

支持以下转义规则：

| 转义规则                 | 说明                 |
| -------------------- | ------------------ |
| `CSV`, `JSON`, `XML` | 类似于同名格式            |
| `Escaped`            | 类似于 `TSV`          |
| `Quoted`             | 类似于 `Values`       |
| `Raw`                | 不进行转义，类似于 `TSVRaw` |
| `None`               | 无转义规则——见下方说明       |

:::note
如果省略转义规则，则会使用 `None`。`XML` 仅适用于输出。
:::

来看一个示例。给定以下格式字符串：

```text
Search phrase: ${s:Quoted}, count: ${c:Escaped}, ad price: $$${p:JSON};
```

以下值会被输出 (如果使用 `SELECT`) 或作为预期输入 (如果使用 `INPUT`) ，
分别位于列 `Search phrase:`、`, count:`、`, ad price: $` 与 `;` 分隔符之间：

* `s` (转义规则为 `Quoted`) 
* `c` (转义规则为 `Escaped`) 
* `p` (转义规则为 `JSON`) 

例如：

* 如果执行 `INSERT`，下面这一行符合预期模板，并会将值 `bathroom interior design`、`2166`、`$3` 读入列 `Search phrase`、`count`、`ad price`。
* 如果执行 `SELECT`，则下面这一行就是输出结果，前提是值 `bathroom interior design`、`2166`、`$3` 已经存储在某个表的列 `Search phrase`、`count`、`ad price` 中。

```yaml
Search phrase: 'bathroom interior design', count: 2166, ad price: $3;
```

<div id="format_template_rows_between_delimiter">
  ### format_template_rows_between_delimiter
</div>

设置 `format_template_rows_between_delimiter` 用于指定行与行之间的分隔符。除最后一行外，每一行后都会输出 (或期望输入) 该分隔符 (默认为 `\n`) 。

<div id="format_template_resultset">
  ### format_template_resultset
</div>

设置 `format_template_resultset` 用于指定一个文件路径，该文件包含结果集的格式字符串。

结果集的格式字符串与行格式字符串的语法相同。
它支持指定前缀、后缀以及输出附加信息的方式，并使用以下占位符代替列名：

* `data` 是采用 `format_template_row` 格式的数据行，由 `format_template_rows_between_delimiter` 分隔。该占位符必须是格式字符串中的第一个占位符。
* `totals` 是采用 `format_template_row` 格式的总计值行 (使用 WITH TOTALS 时) 。
* `min` 是采用 `format_template_row` 格式的最小值行 (当 extremes 设置为 1 时) 。
* `max` 是采用 `format_template_row` 格式的最大值行 (当 extremes 设置为 1 时) 。
* `rows` 是输出行总数。
* `rows_before_limit` 是如果没有 LIMIT 时至少会有的行数。仅当查询包含 LIMIT 时才会输出。如果查询包含 GROUP BY，则 rows&#95;before&#95;limit&#95;at&#95;least 是如果没有 LIMIT 时会有的精确行数。
* `time` 是请求执行时间，单位为秒。
* `rows_read` 是已读取的行数。
* `bytes_read` 是已读取的字节数 (未压缩) 。

占位符 `data`、`totals`、`min` 和 `max` 不能指定转义规则 (或者必须显式指定 `None`) 。其余占位符可以指定任意转义规则。

:::note
如果 `format_template_resultset` 设置为空字符串，则默认值为 `${data}`。
:::

对于 insert 查询，这种格式允许在有前缀或后缀时跳过某些列或字段 (参见示例) 。

<div id="inline_specification">
  ### 内联规范
</div>

很多时候，要将 Template 格式的格式配置
 (由 `format_template_row`、`format_template_resultset` 设置) 部署到集群中所有节点上的某个目录，既有挑战，甚至可能根本无法实现。
此外，这种格式也可能非常简单，完全没必要放在文件中。

在这种情况下，可以使用 `format_template_row_format` (对应 `format_template_row`) 和 `format_template_resultset_format` (对应 `format_template_resultset`) 直接在查询中设置模板字符串，
而不是将其指定为包含该内容的文件路径。

:::note
格式字符串和转义序列的规则与以下内容相同：

* 使用 `format_template_row_format` 时，与 [`format_template_row`](#format_template_row) 相同。
* 使用 `format_template_resultset_format` 时，与 [`format_template_resultset`](#format_template_resultset) 相同。
  :::

<div id="example-usage">
  ## 示例用法
</div>

下面通过两个示例来看看如何使用 `Template` 格式：先介绍如何选择数据，再介绍如何插入数据。

<div id="selecting-data">
  ### 查询数据
</div>

```sql title="Query"
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

```text title="/some/path/resultset.format"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

```text title="/some/path/row.format"
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

```html title="Response"
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>clickhouse</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

<div id="inserting-data">
  ### 插入数据
</div>

```text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

```sql
INSERT INTO UserActivity SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
FORMAT Template
```

```text title="/some/path/resultset.format"
Some header\n${data}\nTotal rows: ${:CSV}\n
```

```text title="/some/path/row.format"
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

占位符中的 `PageViews`、`UserID`、`Duration` 和 `Sign` 是表中的列名。行内 `Useless field` 之后的值，以及后缀中 `\nTotal rows:` 之后的值都会被忽略。
输入数据中的所有分隔符都必须与指定格式字符串中的分隔符完全一致。

<div id="inline_specification">
  ### 内联规范
</div>

还在为手动格式化 markdown 表格而烦恼吗？本示例将介绍如何使用 `Template` 格式和内联规范设置来完成一个简单任务——从 `system.formats` 表中 `SELECT` 一些 ClickHouse 格式的名称，并将其格式化为 markdown 表格。借助 `Template` 格式以及 `format_template_row_format` 和 `format_template_resultset_format` 这两个设置，就能轻松实现这一点。

在前面的示例中，我们分别在单独的文件中指定结果集和行的 格式字符串，并通过 `format_template_resultset` 和 `format_template_row` 设置分别指定这些文件的 path。这里我们改为使用内联方式，因为这个模板非常简单，只需少量 `|` 和 `-` 就能构造出 markdown 表格。我们将使用 `format_template_resultset_format` 设置来指定结果集的 template string。为了生成表头，我们在 `${data}` 前添加了 `|ClickHouse Formats|\n|---|\n`。我们使用 `format_template_row_format` 设置为各行指定模板字符串 ``|`{0:XML}`|``。`Template` 格式会将按给定格式生成的各行插入占位符 `${data}` 中。本示例中只有一列，但如果你想添加更多列，也可以在行模板字符串中加入 `{1:XML}`、`{2:XML}` 等，并根据需要选择合适的 转义规则。本示例中我们选择了 `XML` 转义规则。

```sql title="Query"
WITH formats AS
(
 SELECT * FROM system.formats
 ORDER BY rand()
 LIMIT 5
)
SELECT * FROM formats
FORMAT Template
SETTINGS
 format_template_row_format='|`${0:XML}`|',
 format_template_resultset_format='|ClickHouse Formats|\n|---|\n${data}\n'
```

看看！这样一来，我们就省去了手动添加那些 `|` 和 `-` 来制作这个 markdown 表的麻烦：

```response title="Response"
|ClickHouse Formats|
|---|
|`BSONEachRow`|
|`CustomSeparatedWithNames`|
|`Prometheus`|
|`DWARF`|
|`Avro`|
```