---
toc_folder_title: Table Functions
toc_priority: 34
toc_title: Introduction
---

# Table Functions {#table-functions}

Table functions are methods for constructing tables.

You can use table functions in:

-   [FROM](../statements/select.md#select-from) clause of the `SELECT` query.

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [CREATE TABLE AS \<table\_function()\>](../statements/create.md#create-table-query) query.

        It's one of the methods of creating a table.

!!! warning "Warning"
    You can’t use table functions if the [allow\_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) setting is disabled.

| Function              | Description                                                                                                                            |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [file](file.md)       | Creates a [File](../../engines/table-engines/special/file.md)-engine table.                                                            |
| [merge](merge.md)     | Creates a [Merge](../../engines/table-engines/special/merge.md)-engine table.                                                          |
| [numbers](numbers.md) | Creates a table with a single column filled with integer numbers.                                                                      |
| [remote](remote.md)   | Allows you to access remote servers without creating a [Distributed](../../engines/table-engines/special/distributed.md)-engine table. |
| [url](url.md)         | Creates a [Url](../../engines/table-engines/special/url.md)-engine table.                                                              |
| [mysql](mysql.md)     | Creates a [MySQL](../../engines/table-engines/integrations/mysql.md)-engine table.                                                     |
| [jdbc](jdbc.md)       | Creates a [JDBC](../../engines/table-engines/integrations/jdbc.md)-engine table.                                                       |
| [odbc](odbc.md)       | Creates a [ODBC](../../engines/table-engines/integrations/odbc.md)-engine table.                                                       |
| [hdfs](hdfs.md)       | Creates a [HDFS](../../engines/table-engines/integrations/hdfs.md)-engine table.                                                       |

[Original article](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
