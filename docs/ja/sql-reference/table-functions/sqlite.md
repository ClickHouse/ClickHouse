---
slug: /ja/sql-reference/table-functions/sqlite
sidebar_position: 185
sidebar_label: sqlite
title: sqlite
---

[SQLite](../../engines/database-engines/sqlite.md) データベースに保存されたデータに対してクエリを実行できるようにします。

**構文**

``` sql
    sqlite('db_path', 'table_name')
```

**引数**

- `db_path` — SQLiteデータベースを含むファイルへのパス。[String](../../sql-reference/data-types/string.md)。
- `table_name` — SQLiteデータベース内のテーブルの名前。[String](../../sql-reference/data-types/string.md)。

**返される値**

- 元の `SQLite` テーブルと同じカラムを持つテーブルオブジェクト。

**例**

クエリ:

``` sql
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

結果:

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

**関連項目**

- [SQLite](../../engines/table-engines/integrations/sqlite.md) テーブルエンジン

