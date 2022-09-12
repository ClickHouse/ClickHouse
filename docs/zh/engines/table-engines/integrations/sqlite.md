---
toc_priority: 7
toc_title: SQLite
---

# SQLite {#sqlite}

该引擎允许导入和导出数据到SQLite，并支持查询SQLite表直接从ClickHouse。

## 创建数据表 {#creating-a-table}

``` sql
    CREATE TABLE [IF NOT EXISTS] [db.]table_name 
    (
        name1 [type1], 
        name2 [type2], ...
    ) ENGINE = SQLite('db_path', 'table')
```

**引擎参数**

-   `db_path` — SQLite数据库文件的具体路径地址。
-   `table` — SQLite数据库中的表名。

## 使用示例 {#usage-example}

显示创建表的查询语句：

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

从数据表查询数据：

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

**详见**

-   [SQLite](../../../engines/database-engines/sqlite.md) 引擎
-   [sqlite](../../../sql-reference/table-functions/sqlite.md) 表方法函数