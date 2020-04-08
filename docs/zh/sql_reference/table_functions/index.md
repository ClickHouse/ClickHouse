---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_folder_title: "\u8868\u51FD\u6570"
toc_priority: 34
toc_title: "\u5BFC\u8A00"
---

# 表函数 {#table-functions}

表函数是构造表的方法。

您可以使用表函数:

-   [FROM](../statements/select.md#select-from) 《公约》条款 `SELECT` 查询。

        The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

-   [创建表为\<table\_function()\>](../statements/create.md#create-table-query) 查询。

        It's one of the methods of creating a table.

!!! warning "警告"
    你不能使用表函数，如果 [allow\_ddl](../../operations/settings/permissions_for_queries.md#settings_allow_ddl) 设置被禁用。

| 功能               | 产品描述                                                                                               |
|--------------------|--------------------------------------------------------------------------------------------------------|
| [文件](file.md)    | 创建一个 [文件](../../engines/table_engines/special/file.md)-发动机表。                                |
| [合并](merge.md)   | 创建一个 [合并](../../engines/table_engines/special/merge.md)-发动机表。                               |
| [数字](numbers.md) | 创建一个包含整数填充的单列的表。                                                                       |
| [远程](remote.md)  | 允许您访问远程服务器，而无需创建 [分布](../../engines/table_engines/special/distributed.md)-发动机表。 |
| [url](url.md)      | 创建一个 [Url](../../engines/table_engines/special/url.md)-发动机表。                                  |
| [mysql](mysql.md)  | 创建一个 [MySQL](../../engines/table_engines/integrations/mysql.md)-发动机表。                         |
| [jdbc](jdbc.md)    | 创建一个 [JDBC](../../engines/table_engines/integrations/jdbc.md)-发动机表。                           |
| [odbc](odbc.md)    | 创建一个 [ODBC](../../engines/table_engines/integrations/odbc.md)-发动机表。                           |
| [hdfs](hdfs.md)    | 创建一个 [HDFS](../../engines/table_engines/integrations/hdfs.md)-发动机表。                           |

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/) <!--hide-->
