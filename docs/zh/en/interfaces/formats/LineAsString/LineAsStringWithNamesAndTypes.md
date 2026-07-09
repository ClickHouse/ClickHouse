---
alias: []
description: 'LineAsStringWithNamesAndTypes 格式文档'
input_format: false
keywords: ['LineAsStringWithNamesAndTypes']
output_format: true
slug: /interfaces/formats/LineAsStringWithNamesAndTypes
title: 'LineAsStringWithNamesAndTypes'
doc_type: 'reference'
---

| 输入 | 输出 | 别名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 说明
</div>

`LineAsStringWithNames` 格式与 [`LineAsString`](./LineAsString.md) 格式类似，
但会输出两行表头：一行显示列名，另一行显示类型。

<div id="example-usage">
  ## 使用示例
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNamesAndTypes;
```

```response title="Response"
name    value
String    Int32
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## 格式设置
</div>
