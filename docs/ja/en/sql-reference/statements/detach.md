---
description: 'DETACH のドキュメント'
sidebar_label: 'DETACH'
sidebar_position: 43
slug: /sql-reference/statements/detach
title: 'DETACHステートメント'
doc_type: 'reference'
---

サーバーに、テーブル、materialized view、Dictionary、またはデータベースが存在することを「忘れさせ」ます。

**構文**

```sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

デタッチしても、テーブル、materialized view、Dictionary、データベースのデータやメタデータは削除されません。エンティティが `PERMANENTLY` 付きでデタッチされていない場合、次回のサーバー起動時にサーバーがメタデータを読み込み、そのテーブル／ビュー／Dictionary／データベースを再度認識します。エンティティが `PERMANENTLY` 付きでデタッチされていた場合、自動的には再認識されません。

テーブル、Dictionary、データベースが永続的にデタッチされているかどうかにかかわらず、いずれの場合も [ATTACH](../../sql-reference/statements/attach.md) クエリを使って再アタッチできます。
システムのログテーブルも再アタッチできます (例: `query_log`、`text_log` など) 。そのほかのシステムテーブルは再アタッチできません。次回のサーバー起動時に、サーバーはそれらのテーブルを再び認識します。

`ATTACH MATERIALIZED VIEW` は短い構文 (`SELECT` なし) では動作しませんが、`ATTACH TABLE` クエリを使えばアタッチできます。

なお、すでに (一時的に) デタッチされているテーブルを永続的にデタッチすることはできません。ただし、いったんアタッチし直してから、あらためて永続的にデタッチすることはできます。

また、デタッチされたテーブルを [DROP](../../sql-reference/statements/drop.md#drop-table) したり、永続的にデタッチされたものと同じ名前で [CREATE TABLE](../../sql-reference/statements/create/table.md) したり、[RENAME TABLE](../../sql-reference/statements/rename.md) クエリで別のテーブルに置き換えたりすることもできません。

`SYNC` 修飾子は、遅延なくアクションを実行します。

**例**

テーブルを作成します:

```sql title="Query"
CREATE TABLE test ENGINE = MergeTree ORDER BY () AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

```text title="Response"
┌─number─┐
│      0 │
│      1 │
│      2 │
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
│      9 │
└────────┘
```

テーブルをデタッチする:

```sql title="Query"
DETACH TABLE test;
SELECT * FROM test;
```

```text title="Response"
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

:::note
ClickHouse Cloud では、`PERMANENTLY` 句 (例: `DETACH TABLE <table> PERMANENTLY`) を使用してください。この句を使用しない場合、クラスターの再起動時 (アップグレード時など) にテーブルが再アタッチされます。
:::

**関連項目**

* [Materialized View](/ja/sql-reference/statements/create/view#materialized-view)
* [Dictionaries](./create/dictionary/overview.md)