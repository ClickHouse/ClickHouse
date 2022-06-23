---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。表 {#system-tables}

包含服务器知道的每个表的元数据。 分离的表不显示在 `system.tables`。

此表包含以下列（列类型显示在括号中):

-   `database` (String) — 表所在的数据库表名。

-   `name` (String) — 表名。

-   `engine` (String) — 表引擎名 (不包含参数)。

-   `is_temporary` (UInt8)-指示表是否是临时的标志。

-   `data_path` (String)-文件系统中表数据的路径。

-   `metadata_path` (String)-文件系统中表元数据的路径。

-   `metadata_modification_time` (DateTime)-表元数据的最新修改时间。

-   `dependencies_database` (数组(字符串))-数据库依赖关系。

-   `dependencies_table` （数组（字符串））-表依赖关系 ([MaterializedView](../../engines/table-engines/special/materializedview.md) 基于当前表的表）。

-   `create_table_query` (String)-用于创建表的SQL语句。

-   `engine_full` (String)-表引擎的参数。

-   `partition_key` (String)-表中指定的分区键表达式。

-   `sorting_key` (String)-表中指定的排序键表达式。

-   `primary_key` (String)-表中指定的主键表达式。

-   `sampling_key` (String)-表中指定的采样键表达式。

-   `storage_policy` (字符串)-存储策略:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [分布](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64))-总行数，如果可以快速确定表中的确切行数，否则行数为`Null`（包括底层 `Buffer` 表）。

-   `total_bytes` (Nullable(UInt64))-总字节数，如果可以快速确定存储表的确切字节数，否则字节数为`Null` (即**不** 包括任何底层存储）。

    -   如果表将数据存在磁盘上，返回实际使用的磁盘空间（压缩后）。
    -   如果表在内存中存储数据，返回在内存中使用的近似字节数。

-   `lifetime_rows` (Nullbale(UInt64))-服务启动后插入的总行数(只针对`Buffer`表）。 

`system.tables` 表被用于 `SHOW TABLES` 的查询实现中。

[原文](https://clickhouse.tech/docs/zh/operations/system-tables/tables) <!--hide-->
