---
description: 'SQLiteデータベースに格納されたデータに対してクエリを実行できます。'
sidebar_label: 'sqlite'
sidebar_position: 185
slug: /sql-reference/table-functions/sqlite
title: 'sqlite'
doc_type: 'reference'
---

[SQLite](../../engines/database-engines/sqlite.md)データベースに格納されたデータに対してクエリを実行できます。

<div id="syntax">
  ## 構文
</div>

```sql
sqlite('db_path', 'table_name')
```

<div id="arguments">
  ## 引数
</div>

* `db_path` — SQLite データベースが格納されたファイルへのパス。[String](../../sql-reference/data-types/string.md)。
* `table_name` — SQLite データベース内のテーブル名、またはそのまま SQLite に渡されるクエリ ([テーブル名の代わりにクエリを渡す](#passing-a-query)を参照) 。[String](../../sql-reference/data-types/string.md)。

<div id="returned_value">
  ## 戻り値
</div>

* 元の `SQLite` テーブルと同じカラムを持つテーブルオブジェクト。

<div id="passing-a-query">
  ## テーブル名の代わりにクエリを渡す
</div>

テーブル名の代わりに、第2引数には SQLite にそのまま渡される `SELECT` クエリを指定できます。生成されるテーブルの構造は、クエリ結果から推論されます。クエリは、サブクエリとして記述することも、`query` 関数でラップすることもできます。

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

このようなテーブルは読み取り専用であり、`INSERT` は許可されていません。同じ構文は [`SQLite`](/ja/engines/table-engines/integrations/sqlite) テーブルエンジンでもサポートされています。

:::note
サブクエリ形式 `(SELECT ...)` は、SQLite に送信される前に ClickHouse でパースおよび再シリアライズされます。そのため、有効な ClickHouse SQL である必要があります。ClickHouse がパースしない SQLite 固有の構文を渡すには、`query('...')` 形式を使用してください。この形式では、テキストがそのまま SQLite に送信されます。

囲んでいる ClickHouse クエリの外側にある `WHERE`、`LIMIT`、集約などは、渡されたクエリには **プッシュダウンされず**、完全なクエリ結果を取得した後に ClickHouse 側で適用されます。SQLite から読み取るデータを制限するには、フィルタを渡すクエリ内に記述してください。[`external_table_strict_query = 1`](/ja/operations/settings/settings#external_table_strict_query) を指定すると、プッシュダウンできない外側のフィルタはローカルで適用される代わりに、例外として拒否されます。
:::

<div id="example">
  ## 例
</div>

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

<div id="related">
  ## 関連
</div>

* [SQLite](../../engines/table-engines/integrations/sqlite.md) テーブルエンジン
* [SQLiteデータベースエンジン](../../engines/database-engines/sqlite.md) — データ型のサポート セクション