---
alias: []
description: 'JSONAsString 格式文档'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✔  | ✗  |    |

<div id="description">
  ## 描述
</div>

在这种格式中，单个 JSON 对象会被解释为单个值。
如果输入包含多个 JSON 对象 (以逗号分隔) ，它们会被解释为独立的行。
如果输入数据包含在 `[]` 中，则会被解释为 JSON 对象数组。

:::note
这种格式只能解析为仅包含一个 [String](/zh/sql-reference/data-types/string.md) 类型字段的表。
其余列必须设置为 [`DEFAULT`](/zh/sql-reference/statements/create/table.md/#default) 或 [`MATERIALIZED`](/zh/sql-reference/statements/create/view#materialized-view)，
或者省略。
:::

将整个 JSON 对象序列化为 String 后，就可以使用 [JSON 函数](/zh/sql-reference/functions/json-functions.md) 对其进行处理。

<div id="example-usage">
  ## 使用示例
</div>

<div id="basic-example">
  ### 简单示例
</div>

```sql title="Query"
DROP TABLE IF EXISTS json_as_string;
CREATE TABLE json_as_string (json String) ENGINE = Memory;
INSERT INTO json_as_string (json) FORMAT JSONAsString {"foo":{"bar":{"x":"y"},"baz":1}},{},{"any json stucture":1}
SELECT * FROM json_as_string;
```

```response title="Response"
┌─json──────────────────────────────┐
│ {"foo":{"bar":{"x":"y"},"baz":1}} │
│ {}                                │
│ {"any json stucture":1}           │
└───────────────────────────────────┘
```

<div id="an-array-of-json-objects">
  ### JSON 对象数组
</div>

```sql title="Query"
CREATE TABLE json_square_brackets (field String) ENGINE = Memory;
INSERT INTO json_square_brackets FORMAT JSONAsString [{"id": 1, "name": "name1"}, {"id": 2, "name": "name2"}];

SELECT * FROM json_square_brackets;
```

```response title="Response"
┌─field──────────────────────┐
│ {"id": 1, "name": "name1"} │
│ {"id": 2, "name": "name2"} │
└────────────────────────────┘
```

<div id="format-settings">
  ## 格式设置
</div>
