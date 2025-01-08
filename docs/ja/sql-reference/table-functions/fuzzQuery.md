---
slug: /ja/sql-reference/table-functions/fuzzQuery
sidebar_position: 75
sidebar_label: fuzzQuery
---

# fuzzQuery

指定されたクエリ文字列をランダムなバリエーションで撹乱します。

``` sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

**引数**

- `query` (String) - ファズ化を行う元のクエリ。
- `max_query_length` (UInt64) - ファズ化プロセス中にクエリが到達できる最大長。
- `random_seed` (UInt64) - 安定した結果を出力するためのランダムシード。

**返される値**

擾乱されたクエリ文字列を含む単一カラムのテーブルオブジェクト。

## 使用例

``` sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```
