---
toc_priority: 55
toc_title: sqlite
---

## sqlite {#sqlite}

Allows to perform queries on a data stored in an [SQLite](../../engines/database-engines/sqlite.md) database.

**Syntax** 

``` sql
    sqlite('db_path', 'table_name')
```

**Arguments** 

-   `db_path` — Path to SQLite file with the database. [String](../../sql-reference/data-types/string.md).
-   `table_name` — The SQLite table name. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   A table object with the same columns as the original `SQLite` table.

**Example**

Query:

``` sql
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

Result:

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

**See Also** 

-   [SQLite](../../engines/table-engines/integrations/sqlite.md) table engine