---
description: '指定されたクエリ文字列にランダムな変更を加えます。'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

指定されたクエリ文字列にランダムな変更を加えます。

<div id="syntax">
  ## 構文
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## 引数
</div>

| 引数                 | 説明                               |
| ------------------ | -------------------------------- |
| `query`            | (String) - ファジング対象の元のクエリ。        |
| `max_query_length` | (UInt64) - ファジング処理中にクエリが取り得る最大長。 |
| `random_seed`      | (UInt64) - 結果を安定させるためのランダムシード。   |

<div id="returned_value">
  ## 戻り値
</div>

摂動を加えたクエリ文字列を格納した単一カラムのテーブルオブジェクト。

<div id="usage-example">
  ## 使用例
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```