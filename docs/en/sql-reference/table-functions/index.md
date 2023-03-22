---
sidebar_label: Table Functions
sidebar_position: 34
---

# Table Functions 

Table functions are methods for constructing tables.

You can use table functions in:

-  [FROM](../../sql-reference/statements/select/from.md) clause of the `SELECT` query.

   The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [CREATE TABLE AS table_function()](../../sql-reference/statements/create/table.md) query.

   It's one of the methods of creating a table.

-   [INSERT INTO TABLE FUNCTION](../../sql-reference/statements/insert-into.md#inserting-into-table-function) query.

:::warning
You can’t use table functions if the [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) setting is disabled.
:::

| Function                                                         | Description                                                                                                                            |
|------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [file](../../sql-reference/table-functions/file.md)              | Creates a [File](../../engines/table-engines/special/file.md)-engine table.                                                            |
| [merge](../../sql-reference/table-functions/merge.md)            | Creates a [Merge](../../engines/table-engines/special/merge.md)-engine table.                                                          |
| [numbers](../../sql-reference/table-functions/numbers.md)        | Creates a table with a single column filled with integer numbers.                                                                      |
| [remote](../../sql-reference/table-functions/remote.md)          | Allows you to access remote servers without creating a [Distributed](../../engines/table-engines/special/distributed.md)-engine table. |
| [url](../../sql-reference/table-functions/url.md)                | Creates a [Url](../../engines/table-engines/special/url.md)-engine table.                                                              |
| [mysql](../../sql-reference/table-functions/mysql.md)            | Creates a [MySQL](../../engines/table-engines/integrations/mysql.md)-engine table.                                                     |
| [postgresql](../../sql-reference/table-functions/postgresql.md)  | Creates a [PostgreSQL](../../engines/table-engines/integrations/postgresql.md)-engine table.                                           |
| [jdbc](../../sql-reference/table-functions/jdbc.md)              | Creates a [JDBC](../../engines/table-engines/integrations/jdbc.md)-engine table.                                                       |
| [odbc](../../sql-reference/table-functions/odbc.md)              | Creates a [ODBC](../../engines/table-engines/integrations/odbc.md)-engine table.                                                       |
| [hdfs](../../sql-reference/table-functions/hdfs.md)              | Creates a [HDFS](../../engines/table-engines/integrations/hdfs.md)-engine table.                                                       |
| [s3](../../sql-reference/table-functions/s3.md)                  | Creates a [S3](../../engines/table-engines/integrations/s3.md)-engine table.                                                           |
| [sqlite](../../sql-reference/table-functions/sqlite.md)          | Creates a [sqlite](../../engines/table-engines/integrations/sqlite.md)-engine table.                                                       |

