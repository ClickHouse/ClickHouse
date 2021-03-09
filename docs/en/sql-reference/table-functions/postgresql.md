---
toc_priority: 54
toc_title: postgresql
---

# postgresql {#postgresql}

Allows `SELECT` and `INSERT` queries to be performed on data that is stored on a remote PostgreSQL server.

**Syntax**

``` sql
postgresql('host:port', 'database', 'table', 'user', 'password')
```

**Arguments**

-   `host:port` — PostgreSQL server address.
-   `database` — Remote database name.
-   `table` — Remote table name.
-   `user` — PostgreSQL user.
-   `password` — User password.

**Returned Value**

A table object with the same columns as the original PostgreSQL table.

!!! info "Note"
    In the `INSERT` query to distinguish table function `postgresql(...)` from table name with column names list, you must use keywords `FUNCTION` or `TABLE FUNCTION`. See examples below. 

**Examples**

Consider the table in PostgreSQL:

``` sql
postgre> CREATE TABLE IF NOT EXISTS test_table (a integer, b text, c integer)
postgre> INSERT INTO test_table (a, b, c) VALUES (1, 2, 3), (4, 5, 6)
```

Selecting data from ClickHouse:

``` sql
SELECT * FROM postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` text
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 4 │ 5 │ 6 │
└───┴───┴───┘
```

Inserting into PostgreSQL from ClickHouse:

```sql
INSERT INTO FUNCTION postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword') (a, b, c) VALUES (7, 8, 9);
SELECT * FROM postgresql('localhost:5432', 'test_database', 'test_table', 'postgres', 'mysecretpassword');
```

``` text
┌─a─┬─b─┬─c─┐
│ 1 │ 2 │ 3 │
│ 4 │ 5 │ 6 │
│ 7 │ 8 │ 9 │
└───┴───┴───┘
```

**See Also**

-   [The PostgreSQL table engine](../../engines/table-engines/integrations/postgresql.md)
-   [Using PostgreSQL as a source of external dictionary](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[Original article](https://clickhouse.tech/docs/en/sql-reference/table-functions/postgresql/) <!--hide-->
