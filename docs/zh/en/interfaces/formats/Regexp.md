---
alias: []
description: 'Regexp 格式文档'
input_format: true
keywords: ['Regexp']
output_format: false
slug: /interfaces/formats/Regexp
title: 'Regexp'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 描述
</div>

`Regex` 格式会按照给定的正则表达式解析导入数据的每一行。

**用法**

[format&#95;regexp](/zh/operations/settings/settings-formats.md/#format_regexp) 设置中的正则表达式会应用于导入数据的每一行。正则表达式中的子模式数量必须与导入数据集中的列数相同。

导入数据中的各行必须以换行符 `'\n'` 或 DOS 风格换行符 `"\r\n"` 分隔。

每个匹配到的子模式内容都会根据 [format&#95;regexp&#95;escaping&#95;rule](/zh/operations/settings/settings-formats.md/#format_regexp_escaping_rule) 设置，使用相应数据类型的方法进行解析。

如果正则表达式与该行不匹配，且 [format&#95;regexp&#95;skip&#95;unmatched](/zh/operations/settings/settings-formats.md/#format_regexp_escaping_rule) 设置为 1，则会静默跳过该行。否则，将抛出异常。

<div id="example-usage">
  ## 使用示例
</div>

假设文件 `data.tsv` 如下：

```text title="data.tsv"
id: 1 array: [1,2,3] string: str1 date: 2020-01-01
id: 2 array: [1,2,3] string: str2 date: 2020-01-02
id: 3 array: [1,2,3] string: str3 date: 2020-01-03
```

以及 `imp_regex_table` 表：

```sql title="Query"
CREATE TABLE imp_regex_table (id UInt32, array Array(UInt32), string String, date Date) ENGINE = Memory;
```

我们将使用以下查询，将前述文件中的数据插入上述表中：

```bash title="Query"
$ cat data.tsv | clickhouse-client  --query "INSERT INTO imp_regex_table SETTINGS format_regexp='id: (.+?) array: (.+?) string: (.+?) date: (.+?)', format_regexp_escaping_rule='Escaped', format_regexp_skip_unmatched=0 FORMAT Regexp;"
```

现在我们可以从该表中 `SELECT` 数据，看看 `Regex` 格式如何解析文件中的数据：

```sql title="Query"
SELECT * FROM imp_regex_table;
```

```text title="Response"
┌─id─┬─array───┬─string─┬───────date─┐
│  1 │ [1,2,3] │ str1   │ 2020-01-01 │
│  2 │ [1,2,3] │ str2   │ 2020-01-02 │
│  3 │ [1,2,3] │ str3   │ 2020-01-03 │
└────┴─────────┴────────┴────────────┘
```

<div id="format-settings">
  ## 格式设置
</div>

使用 `Regexp` 格式时，可以使用以下设置：

* `format_regexp` — [String](/zh/sql-reference/data-types/string.md)。包含采用 [re2](https://github.com/google/re2/wiki/Syntax) 格式的正则表达式。

* `format_regexp_escaping_rule` — [String](/zh/sql-reference/data-types/string.md)。支持以下转义规则：

  * CSV (类似于 [CSV](/zh/interfaces/formats/CSV)
  * JSON (类似于 [JSONEachRow](/zh/interfaces/formats/JSONEachRow)
  * Escaped (类似于 [TSV](/zh/interfaces/formats/TabSeparated)
  * Quoted (类似于 [Values](/zh/interfaces/formats/Values)
  * Raw (将子模式整体提取，不使用转义规则，类似于 [TSVRaw](/zh/interfaces/formats/TabSeparated)

* `format_regexp_skip_unmatched` — [UInt8](/zh/sql-reference/data-types/int-uint.md)。用于定义当 `format_regexp` 表达式与导入数据不匹配时，是否抛出异常。可设置为 `0` 或 `1`。