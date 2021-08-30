---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。表 {#system-tables}

包含服务器知道的每个表的元数据。 分离的表不显示在 `system.tables`.

此表包含以下列（列类型显示在括号中):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8)-指示表是否是临时的标志。

-   `data_path` (String)-文件系统中表数据的路径。

-   `metadata_path` (String)-文件系统中表元数据的路径。

-   `metadata_modification_time` (DateTime)-表元数据的最新修改时间。

-   `dependencies_database` (数组(字符串))-数据库依赖关系.

-   `dependencies_table` （数组（字符串））-表依赖关系 ([MaterializedView](../../engines/table-engines/special/materializedview.md) 基于当前表的表）。

-   `create_table_query` (String)-用于创建表的查询。

-   `engine_full` (String)-表引擎的参数。

-   `partition_key` (String)-表中指定的分区键表达式。

-   `sorting_key` (String)-表中指定的排序键表达式。

-   `primary_key` (String)-表中指定的主键表达式。

-   `sampling_key` (String)-表中指定的采样键表达式。

-   `storage_policy` (字符串)-存储策略:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [分布](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64))-总行数，如果可以快速确定表中的确切行数，否则 `Null` （包括内衣 `Buffer` 表）。

-   `total_bytes` (Nullable(UInt64))-总字节数，如果可以快速确定存储表的确切字节数，否则 `Null` (**不** 包括任何底层存储）。

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   如果表在内存中存储数据,返回在内存中使用的近似字节数.

该 `system.tables` 表中使用 `SHOW TABLES` 查询实现。
