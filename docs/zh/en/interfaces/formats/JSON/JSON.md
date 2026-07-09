---
alias: []
description: 'JSON 格式说明文档'
input_format: true
keywords: ['JSON']
output_format: true
slug: /interfaces/formats/JSON
title: 'JSON'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

`JSON` 格式以 JSON 格式读取和输出数据。

`JSON` 格式返回以下内容：

| Parameter                    | Description                                                                                                                                                     |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meta`                       | 列名和类型。                                                                                                                                                          |
| `data`                       | 数据表。                                                                                                                                                            |
| `rows`                       | 输出的总行数。                                                                                                                                                         |
| `rows_before_limit_at_least` | 如果没有 LIMIT，本应产生的行数的下限估计值。仅当查询包含 LIMIT 时才会输出。该估计值基于查询管道中在 limit transform 之前处理过的数据块计算得出，但这些块之后仍可能被 limit transform 丢弃。如果这些块在查询管道中甚至没有到达 limit transform，则不会参与估算。 |
| `statistics`                 | 统计信息，例如 `elapsed`、`rows_read`、`bytes_read`。                                                                                                                     |
| `totals`                     | 总计值 (使用 WITH TOTALS 时) 。                                                                                                                                        |
| `extremes`                   | 极值 (当 extremes 设置为 1 时) 。                                                                                                                                       |

`JSON` 类型与 JavaScript 兼容。为确保这一点，部分字符会额外进行转义：

* 斜杠 `/` 会被转义为 `\/`
* 某些浏览器无法处理的替代换行符 `U+2028` 和 `U+2029` 会被转义为 `\uXXXX`。
* ASCII 控制字符会被转义：退格、换页、换行符、回车和水平制表符会分别替换为 `\b`、`\f`、`\n`、`\r`、`\t`，其余位于 00-1F 范围内的字节则使用 `\uXXXX` 序列。
* 无效的 UTF-8 序列会被替换为替代字符 �，因此输出文本将仅包含有效的 UTF-8 序列。

为兼容 JavaScript，Int64 和 UInt64 整数默认会用双引号括起来。
若要去掉引号，可以将配置参数 [`output_format_json_quote_64bit_integers`](/zh/operations/settings/settings-formats.md/#output_format_json_quote_64bit_integers) 设置为 `0`。

ClickHouse 支持 [NULL](/zh/sql-reference/syntax.md)，在 JSON 输出中显示为 `null`。若要在输出中启用 `+nan`、`-nan`、`+inf`、`-inf` 值，请将 [output&#95;format&#95;json&#95;quote&#95;denormals](/zh/operations/settings/settings-formats.md/#output_format_json_quote_denormals) 设置为 `1`。

<div id="example-usage">
  ## 使用示例
</div>

示例：

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "num",
                        "type": "Int32"
                },
                {
                        "name": "str",
                        "type": "String"
                },
                {
                        "name": "arr",
                        "type": "Array(UInt8)"
                }
        ],

        "data":
        [
                {
                        "num": 42,
                        "str": "hello",
                        "arr": [0,1]
                },
                {
                        "num": 43,
                        "str": "hello",
                        "arr": [0,1,2]
                },
                {
                        "num": 44,
                        "str": "hello",
                        "arr": [0,1,2,3]
                }
        ],

        "rows": 3,

        "rows_before_limit_at_least": 3,

        "statistics":
        {
                "elapsed": 0.001137687,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

<div id="format-settings">
  ## 格式设置
</div>

对于 JSON 输入格式，如果将设置 [`input_format_json_validate_types_from_metadata`](/zh/operations/settings/settings-formats.md/#input_format_json_validate_types_from_metadata) 设为 `1`，
则会将输入数据元数据中的类型与表中对应列的类型进行比较。

<div id="see-also">
  ## 另请参阅
</div>

* [JSONEachRow](/zh/interfaces/formats/JSONEachRow) 格式
* [output&#95;format&#95;json&#95;array&#95;of&#95;rows](/zh/operations/settings/settings-formats.md/#output_format_json_array_of_rows) 设置