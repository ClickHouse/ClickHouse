---
slug: /ja/sql-reference/table-functions/
sidebar_label: テーブル関数
sidebar_position: 1
---

# テーブル関数

テーブル関数は、テーブルを構築するための方法です。

テーブル関数は以下で使用できます：

- `SELECT` クエリの [FROM](../../sql-reference/statements/select/from.md) 句。

   現在のクエリ内でのみ使用可能な一時テーブルを作成する方法です。クエリが終了するとテーブルは削除されます。

- [CREATE TABLE AS table_function()](../../sql-reference/statements/create/table.md) クエリ。

   テーブルを作成する方法の一つです。

- [INSERT INTO TABLE FUNCTION](../../sql-reference/statements/insert-into.md#inserting-into-table-function) クエリ。

:::note
[allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) 設定が無効の場合、テーブル関数を使用することはできません。
:::
