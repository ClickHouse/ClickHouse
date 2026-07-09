---
description: '現在のセッションで定義されている仮想索引を一覧表示するシステムテーブル'
keywords: ['システムテーブル', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

現在のセッションで定義されている、すべての仮想的な (what-if) スキップ索引を一覧表示します。[`CREATE HYPOTHETICAL INDEX`](/ja/sql-reference/statements/hypothetical-index#create-hypothetical-index) および [`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif) を参照してください。

内容はセッション単位です。各接続から見えるのはその接続自身の仮想索引のみで、現在のセッションで索引が1つも作成されていない場合、このテーブルは空です。

現在の `(database, table)` はクエリ実行時に UUID によって解決されるため、`RENAME TABLE` が反映され、削除されたテーブルのエントリは自動的に非表示になります。

<div id="columns">
  ## カラム
</div>

| Column        | Type     | Description                                |
| ------------- | -------- | ------------------------------------------ |
| `database`    | `String` | 対象データベース。                                  |
| `table`       | `String` | 対象テーブル。                                    |
| `name`        | `String` | 索引の名前。                                     |
| `type`        | `String` | 索引タイプ (`minmax`、`set`、`bloom_filter` など) 。 |
| `type_full`   | `String` | 引数を含む索引タイプの式 (例: `bloom_filter(0.01)`) 。   |
| `expression`  | `String` | `CREATE HYPOTHETICAL INDEX` に記述された索引式。     |
| `granularity` | `UInt64` | インデックスグラニュールあたりのデータグラニュール数。                |

<div id="example">
  ## 例
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` は基本型の型名で、`type_full` には引数が含まれるため、`bloom_filter(0.01)` と `bloom_filter(0.001)` のようなパラメータ付きのバリアントを区別できます。

<div id="see-also">
  ## 関連項目
</div>

* [`CREATE HYPOTHETICAL INDEX`](/ja/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/ja/sql-reference/statements/explain#explain-whatif)