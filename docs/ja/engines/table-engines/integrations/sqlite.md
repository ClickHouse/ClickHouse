---
slug: /ja/engines/table-engines/integrations/sqlite
sidebar_position: 185
sidebar_label: SQLite
---

# SQLite

このエンジンは、SQLite へのデータのインポートおよびエクスポートを可能にし、ClickHouse から直接 SQLite テーブルへのクエリをサポートします。

## テーブルの作成 {#creating-a-table}

``` sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name
    (
        name1 [type1],
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**エンジンパラメータ**

- `db_path` — データベースを含む SQLite ファイルへのパス。
- `table` — SQLite データベース内のテーブル名。

## 使用例 {#usage-example}

SQLite テーブルを作成するクエリを示します：

```sql
SHOW CREATE TABLE sqlite_db.table2;
```

``` text
CREATE TABLE SQLite.table2
(
    `col1` Nullable(Int32),
    `col2` Nullable(String)
)
ENGINE = SQLite('sqlite.db','table2');
```

テーブルからデータを返します：

``` sql
SELECT * FROM sqlite_db.table2 ORDER BY col1;
```

```text
┌─col1─┬─col2──┐
│    1 │ text1 │
│    2 │ text2 │
│    3 │ text3 │
└──────┴───────┘
```

**関連項目**

- [SQLite](../../../engines/database-engines/sqlite.md) エンジン
- [sqlite](../../../sql-reference/table-functions/sqlite.md) テーブル関数
