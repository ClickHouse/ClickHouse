---
sidebar_label: 表函数
sidebar_position: 34
---

# 表函数 {#table-functions}

表函数是用来构造表的方法。

您可以在以下位置使用表函数:

-    `SELECT` 查询的[FROM](../../sql-reference/statements/select/from.md)子句。

        创建临时表的方法，该临时表仅在当前查询中可用。当查询完成后，该临时表将被删除。

-   [CREATE TABLE AS \<table_function()\>](../statements/create.md#create-table-query) 查询。

        这是创建表的方法之一。

!!! warning "警告"
    如果 [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) 设置被禁用，则不能使用表函数。

| 函数               | 描述                                                                                               |
|-----------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| [file](../../sql-reference/table-functions/file.md)             | 创建一个file引擎表。 |
| [merge](../../sql-reference/table-functions/merge.md)           | 创建一个merge引擎表。 |
| [numbers](../../sql-reference/table-functions/numbers.md)       | 创建一个单列的表，其中包含整数。 |
| [remote](../../sql-reference/table-functions/remote.md)         | 允许您访问远程服务器，而无需创建分布式表。  |
| [url](../../sql-reference/table-functions/url.md)               | 创建一个URL引擎表。 |
| [mysql](../../sql-reference/table-functions/mysql.md)           | 创建一个MySQL引擎表。 |
| [jdbc](../../sql-reference/table-functions/jdbc.md)             | 创建一个JDBC引擎表。 |
| [odbc](../../sql-reference/table-functions/odbc.md)             | 创建一个ODBC引擎表。 |
| [hdfs](../../sql-reference/table-functions/hdfs.md)             | 创建一个HDFS引擎表。 |

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/) <!--hide-->
