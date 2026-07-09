---
description: 'サブクエリをテーブルに変換します。この関数はビューを実現します。'
sidebar_label: 'view'
sidebar_position: 210
slug: /sql-reference/table-functions/view
title: 'view'
doc_type: 'reference'
---

サブクエリをテーブルに変換します。この関数はビューを実現します ([CREATE VIEW](/ja/sql-reference/statements/create/view) を参照) 。このテーブル自体にはデータは保存されず、指定された `SELECT` クエリだけが保存されます。テーブルの読み取り時には、ClickHouse がそのクエリを実行し、結果から不要なカラムをすべて削除します。

<div id="syntax">
  ## 構文
</div>

```sql
view(subquery)
```

<div id="arguments">
  ## 引数
</div>

* `subquery` — `SELECT` クエリ。

<div id="returned_value">
  ## 戻り値
</div>

* テーブル。

<div id="examples">
  ## 例
</div>

入力テーブル:

```text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

```sql title="Query"
SELECT * FROM view(SELECT name FROM months);
```

```text title="Response"
┌─name─────┐
│ January  │
│ February │
│ March    │
│ April    │
└──────────┘
```

[remote](/ja/sql-reference/table-functions/remote) および [cluster](/ja/sql-reference/table-functions/cluster) テーブル関数のパラメーターとして `view` 関数を使用できます:

```sql title="Query"
SELECT * FROM remote(`127.0.0.1`, view(SELECT a, b, c FROM table_name));
```

```sql title="Query"
SELECT * FROM cluster(`cluster_name`, view(SELECT a, b, c FROM table_name));
```

<div id="related">
  ## 関連
</div>

* [Viewテーブルエンジン](/ja/engines/table-engines/special/view/)