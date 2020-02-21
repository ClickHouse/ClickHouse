# Table Functions

Table functions are methods for constructing tables.

You can use table functions in:

* [CREATE TABLE AS <table_function()>](../create.md#create-table-query) query.

    It's one of the methods of creating a table.

* [FROM](../select.md#select-from) clause of the `SELECT` query.

    The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

!!! warning "Warning"
    You can't use table functions if the [allow_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) setting is disabled.

Function | Description
---------|------------
[file](file.md) | Creates a [File](../../operations/table_engines/file.md)-engine table.
[merge](merge.md) | Creates a [Merge](../../operations/table_engines/merge.md)-engine table.
[numbers](numbers.md) | Creates a table with a single column filled with integer numbers.
[remote](remote.md) | Allows you to access remote servers without creating a [Distributed](../../operations/table_engines/distributed.md)-engine table.
[url](url.md) | Creates a [Url](../../operations/table_engines/url.md)-engine table.
[mysql](mysql.md) | Creates a [MySQL](../../operations/table_engines/mysql.md)-engine table.
[jdbc](jdbc.md) | Creates a [JDBC](../../operations/table_engines/jdbc.md)-engine table.
[odbc](odbc.md) | Creates a [ODBC](../../operations/table_engines/odbc.md)-engine table.
[hdfs](hdfs.md) | Creates a [HDFS](../../operations/table_engines/hdfs.md)-engine table.

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/) <!--hide-->
