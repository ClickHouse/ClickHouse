---
toc_folder_title: Table Functions
toc_priority: 34
toc_title: Introduction
---

# Table Functions {#table-functions}

Table functions are methods for constructing tables.

You can use table functions in:

-   [FROM](../../sql-reference/statements/select/from.md) clause of the `SELECT` query.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [CREATE TABLE AS \<table_function()\>](../../sql-reference/statements/create/table.md) query.

        It's one of the methods of creating a table.

!!! warning "Warning"
    You can’t use table functions if the [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) setting is disabled.

| Function                                                        | Description                                                                                                                            |
|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [file](../../sql-reference/table-functions/file.md)             | Creates a File-engine table. |
| [merge](../../sql-reference/table-functions/merge.md)           | Creates a Merge-engine table. |
| [numbers](../../sql-reference/table-functions/numbers.md)       | Creates a table with a single column filled with integer numbers. |
| [remote](../../sql-reference/table-functions/remote.md)         | Allows you to access remote servers without creating a Distributed-engine table. |
| [url](../../sql-reference/table-functions/url.md)               | Creates a URL-engine table. |
| [mysql](../../sql-reference/table-functions/mysql.md)           | Creates a MySQL-engine table. |
| [postgresql](../../sql-reference/table-functions/postgresql.md) | Creates a PostgreSQL-engine table. |
| [jdbc](../../sql-reference/table-functions/jdbc.md)             | Creates a JDBC-engine table. |
| [odbc](../../sql-reference/table-functions/odbc.md)             | Creates a ODBC-engine table. |
| [hdfs](../../sql-reference/table-functions/hdfs.md)             | Creates a HDFS-engine table. |
| [s3](../../sql-reference/table-functions/s3.md)                 | Creates a S3-engine table. |
