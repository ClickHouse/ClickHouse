---
alias: []
description: 'LineAsStringWithNames フォーマットに関するドキュメント'
input_format: false
keywords: ['LineAsStringWithNames']
output_format: true
slug: /interfaces/formats/LineAsStringWithNames
title: 'LineAsStringWithNames'
doc_type: 'reference'
---

| 入力 | 出力 | 別名 |
| -- | -- | -- |
| ✗  | ✔  |    |

<div id="description">
  ## 説明
</div>

`LineAsStringWithNames` フォーマットは [`LineAsString`](./LineAsString.md) フォーマットに似ていますが、カラム名を含むヘッダー行も出力します。

<div id="example-usage">
  ## 使用例
</div>

```sql title="Query"
CREATE TABLE example (
    name String,
    value Int32
)
ENGINE = Memory;

INSERT INTO example VALUES ('John', 30), ('Jane', 25), ('Peter', 35);

SELECT * FROM example FORMAT LineAsStringWithNames;
```

```response title="Response"
name    value
John    30
Jane    25
Peter    35
```

<div id="format-settings">
  ## フォーマット設定
</div>
