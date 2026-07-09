---
alias: []
description: 'LineAsString 格式文档'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: '参考'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 说明
</div>

`LineAsString` 格式会将输入数据的每一行都解析为一个字符串值。
只有在表中仅有一个 [String](/zh/sql-reference/data-types/string.md) 类型字段时，才能解析这种格式。
其余列必须设为 [`DEFAULT`](/zh/sql-reference/statements/create/table.md/#default)、[`MATERIALIZED`](/zh/sql-reference/statements/create/view#materialized-view)，或直接省略。

<div id="example-usage">
  ## 使用示例
</div>

```sql title="Query"
DROP TABLE IF EXISTS line_as_string;
CREATE TABLE line_as_string (field String) ENGINE = Memory;
INSERT INTO line_as_string FORMAT LineAsString "I love apple", "I love banana", "I love orange";
SELECT * FROM line_as_string;
```

```text title="Response"
┌─field─────────────────────────────────────────────┐
│ "I love apple", "I love banana", "I love orange"; │
└───────────────────────────────────────────────────┘
```

<div id="format-settings">
  ## 格式设置
</div>
