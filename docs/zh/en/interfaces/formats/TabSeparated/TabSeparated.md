---
alias: ['TSV']
description: 'TSV 格式说明文档'
input_format: true
keywords: ['TabSeparated', 'TSV']
output_format: true
slug: /interfaces/formats/TabSeparated
title: 'TabSeparated'
doc_type: 'reference'
---

| 输入 | 输出 | 别名    |
| -- | -- | ----- |
| ✔  | ✔  | `TSV` |

<div id="description">
  ## 描述
</div>

在 TabSeparated 格式中，数据按行写入。每一行包含由制表符分隔的值。除每行最后一个值后跟的是换行符外，每个值后面都跟一个制表符。这里严格要求全都使用 Unix 换行符。最后一行末尾也必须包含换行符。值以文本格式写入，不加引号，并且会对特殊字符进行转义。

这种格式也可使用名称 `TSV`。

`TabSeparated` 格式便于使用自定义程序和脚本处理数据。它默认用于 HTTP interface，以及命令行客户端的批次模式。该格式还支持在不同 DBMSs 之间传输数据。例如，你可以从 MySQL 获取转储并将其上传到 ClickHouse，反之亦然。

`TabSeparated` 格式支持输出总计值 (使用 WITH TOTALS 时) 和极值 (当 &#39;extremes&#39; 设为 1 时) 。在这些情况下，总计值和极值会在主体数据之后输出。主结果、总计值和极值之间以空行分隔。示例：

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated

2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div id="tabseparated-data-formatting">
  ## 数据格式化
</div>

整数以十进制形式书写。数字开头可以额外带一个 `+` 字符 (解析时会忽略，格式化时不会保留) 。非负数不能包含负号。读取时，允许将空字符串解析为零，或者 (对于有符号类型) 将仅包含一个负号的字符串解析为零。不符合相应数据类型范围的数字，可能会被解析为另一个数字，且不会报错。

浮点数以十进制形式书写。小数分隔符使用点号。支持指数表示法，也支持 `inf`、`+inf`、`-inf` 和 `nan`。浮点数可以以小数点开头或结尾。
在格式化时，浮点数可能会损失精度。
在解析时，并不严格要求读取最接近机器可表示的数值。

日期以 YYYY-MM-DD 格式书写，并按相同格式解析，但分隔符可以是任意字符。
带时间的日期以 `YYYY-MM-DD hh:mm:ss` 格式书写，并按相同格式解析，但分隔符可以是任意字符。
这些操作都使用客户端或服务器启动时的系统时区 (取决于由哪一方格式化数据) 。对于带时间的日期，夏令时未作规定。因此，如果某个转储包含夏令时期间的时间，该转储就无法与数据明确一一对应，解析时会从两个时间中选择一个。
在读取操作期间，不正确的日期和带时间的日期可以按自然溢出的方式解析，或解析为空日期和时间，且不会报错。

作为一个例外，如果带时间的日期恰好由 10 位十进制数字组成，也支持按 Unix timestamp 格式解析。结果不依赖于时区。格式 `YYYY-MM-DD hh:mm:ss` 和 `NNNNNNNNNN` 会被自动区分。

字符串输出时会对特殊字符进行反斜杠转义。输出时使用以下转义序列：`\b`、`\f`、`\r`、`\n`、`\t`、`\0`、`\'`、`\\`。解析时还支持 `\a`、`\v` 和 `\xHH` (十六进制转义序列) ，以及任意 `\c` 序列，其中 `c` 可以是任意字符 (这些序列会被转换为 `c`) 。因此，读取数据时支持以下格式：换行符可以写成 `\n` 或 `\`，也可以直接写成换行符。例如，字符串 `Hello world` 如果单词之间不是空格而是换行符，则可以按以下任意一种形式解析：

```text
Hello\nworld

Hello\
world
```

支持第二种变体，是因为 MySQL 在写入制表符分隔的转储时会使用它。

以 TabSeparated 格式 传递数据时，最少需要转义的字符有：tab、line feed (LF) 和 backslash。

只有一小部分符号会被转义。你很容易遇到某个字符串值，而终端会在输出时把它破坏掉。

Arrays 写作 `[]` 中以逗号分隔的值列表。数组中的数值项按常规格式化。`Date` 和 `DateTime` types 使用 single quotes。String 也使用 single quotes，并遵循与上述相同的转义规则。

[NULL](/zh/sql-reference/syntax.md) 会按照设置 [format&#95;tsv&#95;null&#95;representation](/zh/operations/settings/settings-formats.md/#format_tsv_null_representation) 进行格式化 (default value 为 `\N`) 。

在输入数据中，ENUM values 可以表示为名称或 id。首先，我们会尝试将 input value 与 ENUM 名称匹配。如果失败，且 input value 是数字，则会尝试将该数字与 ENUM id 匹配。
如果输入数据只包含 ENUM id，建议启用设置 [input&#95;format&#95;tsv&#95;enum&#95;as&#95;number](/zh/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number) 以优化 ENUM parsing。

[Nested](/zh/sql-reference/data-types/nested-data-structures/index.md) 结构的每个元素都表示为数组。

例如：

```sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

```sql
INSERT INTO nestedt VALUES ( 1, [1], ['a'])
```

```sql
SELECT * FROM nestedt FORMAT TSV
```

```response
1  [1]    ['a']
```

<div id="example-usage">
  ## 使用示例
</div>

<div id="inserting-data">
  ### 插入数据
</div>

使用以下名为 `football.tsv` 的 TSV 文件：

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

插入数据：

```sql
INSERT INTO football FROM INFILE 'football.tsv' FORMAT TabSeparated;
```

<div id="reading-data">
  ### 读取数据
</div>

使用 `TabSeparated` 格式读取数据：

```sql
SELECT *
FROM football
FORMAT TabSeparated
```

输出将为制表符分隔格式：

```tsv
2022-04-30      2021    Sutton United   Bradford City   1       4
2022-04-30      2021    Swindon Town    Barrow  2       1
2022-04-30      2021    Tranmere Rovers Oldham Athletic 2       0
2022-05-02      2021    Port Vale       Newport County  1       2
2022-05-02      2021    Salford City    Mansfield Town  2       2
2022-05-07      2021    Barrow  Northampton Town        1       3
2022-05-07      2021    Bradford City   Carlisle United 2       0
2022-05-07      2021    Bristol Rovers  Scunthorpe United       7       0
2022-05-07      2021    Exeter City     Port Vale       0       1
2022-05-07      2021    Harrogate Town A.F.C.   Sutton United   0       2
2022-05-07      2021    Hartlepool United       Colchester United       0       2
2022-05-07      2021    Leyton Orient   Tranmere Rovers 0       1
2022-05-07      2021    Mansfield Town  Forest Green Rovers     2       2
2022-05-07      2021    Newport County  Rochdale        0       2
2022-05-07      2021    Oldham Athletic Crawley Town    3       3
2022-05-07      2021    Stevenage Borough       Salford City    4       2
2022-05-07      2021    Walsall Swindon Town    0       3
```

<div id="format-settings">
  ## 格式设置
</div>

| 设置                                                                                                                                                       | 说明                                                                                                                                                                                      | 默认值     |
| -------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| [`format_tsv_null_representation`](/zh/operations/settings/settings-formats.md/#format_tsv_null_representation)                                             | TSV 格式中的自定义 NULL 表示形式。                                                                                                                                                                  | `\N`    |
| [`input_format_tsv_empty_as_default`](/zh/operations/settings/settings-formats.md/#input_format_tsv_empty_as_default)                                       | 将 TSV 输入中的空字段视为默认值。对于复杂的默认表达式，还必须启用 [input&#95;format&#95;defaults&#95;for&#95;omitted&#95;fields](/zh/operations/settings/settings-formats.md/#input_format_defaults_for_omitted_fields)。 | `false` |
| [`input_format_tsv_enum_as_number`](/zh/operations/settings/settings-formats.md/#input_format_tsv_enum_as_number)                                           | 将在 TSV 格式中 insert 的枚举值视为枚举索引。                                                                                                                                                           | `false` |
| [`input_format_tsv_use_best_effort_in_schema_inference`](/zh/operations/settings/settings-formats.md/#input_format_tsv_use_best_effort_in_schema_inference) | 在 TSV 格式中使用一些调整和启发式方法来推断 schema。禁用后，所有字段都会被推断为 String。                                                                                                                                  | `true`  |
| [`output_format_tsv_crlf_end_of_line`](/zh/operations/settings/settings-formats.md/#output_format_tsv_crlf_end_of_line)                                     | 如果设置为 true，TSV 输出格式中的行尾将使用 `\r\n` 而不是 `\n`。                                                                                                                                             | `false` |
| [`input_format_tsv_crlf_end_of_line`](/zh/operations/settings/settings-formats.md/#input_format_tsv_crlf_end_of_line)                                       | 如果设置为 true，TSV 输入格式中的行尾将使用 `\r\n` 而不是 `\n`。                                                                                                                                             | `false` |
| [`input_format_tsv_skip_first_lines`](/zh/operations/settings/settings-formats.md/#input_format_tsv_skip_first_lines)                                       | 跳过数据开头指定数量的行。                                                                                                                                                                           | `0`     |
| [`input_format_tsv_detect_header`](/zh/operations/settings/settings-formats.md/#input_format_tsv_detect_header)                                             | 自动检测 TSV 格式中包含名称和类型的表头。                                                                                                                                                                 | `true`  |
| [`input_format_tsv_skip_trailing_empty_lines`](/zh/operations/settings/settings-formats.md/#input_format_tsv_skip_trailing_empty_lines)                     | 跳过数据末尾的空行。                                                                                                                                                                              | `false` |
| [`input_format_tsv_allow_variable_number_of_columns`](/zh/operations/settings/settings-formats.md/#input_format_tsv_allow_variable_number_of_columns)       | 允许 TSV 格式中的列数可变，忽略多余的列，并为缺失的列使用默认值。                                                                                                                                                     | `false` |