---
toc_priority: 55
toc_title: sqlite
---

## sqlite {#sqlite}

Allows to performed queries on data that is stored in the `SQLite` database.

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

The example must show usage and/or a use cases. The following text contains recommended parts of an example.

Input table (Optional):

``` text
```

Query:

``` sql
```

Result:

``` text
```

**See Also** 

-   [`SQLite` table engine](../../engines/table-engines/integrations/sqlite.md)