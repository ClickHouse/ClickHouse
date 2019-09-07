# Table functions

Table function is a method of creating a table.

You can call table functions in:

* [CREATE TABLE](../create.md) query.

    It's another method of creating an ordinary table.

* [FROM](../select.md#select-from) clause of the `SELECT` query.

    The method of creating a temporary table, that is deleted after the query finishes.

!!! warning "Warning"
    You cann't use table functions if the [allow_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) setting is disabled.

Тут прямо вопрос, какая таки настройка должна быть allow_ddl или readonly. 

Function | Description
---------|------------
[file](file.md) | Creates a [File](../../operations/table_engines/file.md)-engine table.
[merge](merge.md) | Creates a [Merge](../../operations/table_engines/merge.md)-engine table.
[numbers](numbers.md) | Creates a table with the single column filled with integer numbers.
[remote](remote.md) | Allows you to access remote servers without creating a [Distributed](../../operations/table_engines/distributed.md)-engine table.
[url](url.md) | Creates a [Url](../../operations/table_engines/url.md)-engine table.
[mysql](mysql.md) | Creates a [MySQL](../../operations/table_engines/mysql.md)-engine table.
[jdbc](jdbc.md) | Creates a [JDBC](../../operations/table_engines/jdbc.md)-engine table.
[odbc](odbc.md) | Creates a [ODBC](../../operations/table_engines/odbc.md)-engine table.
[hdfs](hdfs.md) | Creates a [HDFS](../../operations/table_engines/hdfs.md)-engine table.

[Original article](https://clickhouse.yandex/docs/en/query_language/table_functions/) <!--hide-->
