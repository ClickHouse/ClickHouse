---
alias: []
description: 'LineAsStringフォーマットのドキュメント'
input_format: true
keywords: ['LineAsString']
output_format: true
slug: /interfaces/formats/LineAsString
title: 'LineAsString'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✔  |       |

<div id="description">
  ## 説明
</div>

`LineAsString` フォーマットは、入力データの各行を 1 つの文字列値として解釈します。
このフォーマットをパースできるのは、型が [String](/ja/sql-reference/data-types/string.md) の単一のフィールドを持つテーブルに限られます。
それ以外のカラムは、[`DEFAULT`](/ja/sql-reference/statements/create/table.md/#default) または [`MATERIALIZED`](/ja/sql-reference/statements/create/view#materialized-view) に設定するか、省略する必要があります。

<div id="example-usage">
  ## 使用例
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
  ## フォーマットの設定
</div>
