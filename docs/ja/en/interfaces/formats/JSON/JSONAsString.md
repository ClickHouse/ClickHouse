---
alias: []
description: 'JSONAsStringフォーマットのリファレンス'
input_format: true
keywords: ['JSONAsString']
output_format: false
slug: /interfaces/formats/JSONAsString
title: 'JSONAsString'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✔  | ✗  |       |

<div id="description">
  ## 説明
</div>

このフォーマットでは、1 つの JSON オブジェクトは 1 つの値として解釈されます。
入力に複数の JSON オブジェクトが含まれている場合 (カンマ区切り) 、それぞれが отдельの行として解釈されます。
入力データが `[]` で囲まれている場合は、JSON オブジェクトの配列として解釈されます。

:::note
このフォーマットを解析できるのは、[String](/ja/sql-reference/data-types/string.md) 型の単一フィールドを持つテーブルに限られます。
残りのカラムは [`DEFAULT`](/ja/sql-reference/statements/create/table.md/#default) または [`MATERIALIZED`](/ja/sql-reference/statements/create/view#materialized-view) に設定するか、
省略する必要があります。
:::

JSON オブジェクト全体を String にシリアライズすると、[JSON functions](/ja/sql-reference/functions/json-functions.md) を使って処理できます。

<div id="example-usage">
  ## 使用例
</div>

<div id="basic-example">
  ### 基本的な例
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
  ### JSON オブジェクトの配列
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
  ## フォーマット設定
</div>
