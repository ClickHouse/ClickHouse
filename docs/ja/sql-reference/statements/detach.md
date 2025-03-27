---
slug: /ja/sql-reference/statements/detach
sidebar_position: 43
sidebar_label: DETACH
title: "DETACH ステートメント"
---

テーブル、Materialized View、Dictionary、データベースの存在をサーバーに「忘れさせる」ためのコマンドです。

**構文**

``` sql
DETACH TABLE|VIEW|DICTIONARY|DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
```

DETACHはテーブル、Materialized View、Dictionary、データベースのデータやメタデータを削除しません。エンティティが`PERMANENTLY`でない場合、次回サーバー起動時にサーバーはメタデータを読み取り、再びテーブル/View/Dictionary/データベースを認識します。エンティティが`PERMANENTLY`でDETACHされた場合は自動的に認識されません。

テーブル、Dictionary、データベースが恒久的にDETACHされたか否かにかかわらず、[ATTACH](../../sql-reference/statements/attach.md)クエリを使用して再アタッチできます。システムログテーブル（例: `query_log`, `text_log`など）も再アタッチ可能です。他のシステムテーブルは再アタッチできませんが、次回サーバー起動時にサーバーはこれらのテーブルを再認識します。

`ATTACH MATERIALIZED VIEW`は短い構文（`SELECT`なし）では動作しませんが、`ATTACH TABLE`クエリを使用してアタッチすることができます。

既に一時的にDETACHされたテーブルを恒久的にDETACHすることはできません。ただし、一旦アタッチバックしてから再度恒久的にDETACHすることは可能です。

また、DETACHされたテーブルを[DROP](../../sql-reference/statements/drop.md#drop-table)したり、同名のテーブルを[CREATE TABLE](../../sql-reference/statements/create/table.md)で作成したり、[RENAME TABLE](../../sql-reference/statements/rename.md)クエリで他のテーブルに置き換えることはできません。

`SYNC`修飾子を使用すると、アクションを遅延なく実行します。

**例**

テーブルを作成する:

クエリ:

``` sql
CREATE TABLE test ENGINE = Log AS SELECT * FROM numbers(10);
SELECT * FROM test;
```

結果:

``` text
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

テーブルをDETACHする:

クエリ:

``` sql
DETACH TABLE test;
SELECT * FROM test;
```

結果:

``` text
Received exception from server (version 21.4.1):
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.test does not exist.
```

**関連情報**

- [Materialized View](../../sql-reference/statements/create/view.md#materialized)
- [Dictionaries](../../sql-reference/dictionaries/index.md)
