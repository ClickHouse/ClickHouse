# system.tables {#system-tables}

包含服务器知道的每个表的元数据。 [分离的](../../sql-reference/statements/detach.md)表不在 `system.tables` 显示。

[临时表](../../sql-reference/statements/create/table.md#temporary-tables)只在创建它们的会话中的 `system.tables` 中才可见。它们的数据库字段显示为空，并且 `is_temporary` 标志显示为开启。

此表包含以下列 (列类型显示在括号中):

-   `database` ([String](../../sql-reference/data-types/string.md)) — 表所在的数据库名。

-   `name` ([String](../../sql-reference/data-types/string.md)) — 表名。

-   `engine` ([String](../../sql-reference/data-types/string.md)) — 表引擎名 (不包含参数)。

-   `is_temporary` ([UInt8](../../sql-reference/data-types/int-uint.md)) - 指示表是否是临时的标志。

-   `data_path` ([String](../../sql-reference/data-types/string.md)) - 表数据在文件系统中的路径。

-   `metadata_path` ([String](../../sql-reference/data-types/string.md)) - 表元数据在文件系统中的路径。

-   `metadata_modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) - 表元数据的最新修改时间。

-   `dependencies_database` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - 数据库依赖关系。

-   `dependencies_table` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) - 表依赖关系 (基于当前表的 [物化视图](../../engines/table-engines/special/materializedview.md) 表) 。

-   `create_table_query` ([String](../../sql-reference/data-types/string.md)) - 用于创建表的 SQL 语句。

-   `engine_full` ([String](../../sql-reference/data-types/string.md)) - 表引擎的参数。

-   `as_select` ([String](../../sql-reference/data-types/string.md)) - 视图的 `SELECT` 语句。

-   `partition_key` ([String](../../sql-reference/data-types/string.md)) - 表中指定的分区键表达式。

-   `sorting_key` ([String](../../sql-reference/data-types/string.md)) - 表中指定的排序键表达式。

-   `primary_key` ([String](../../sql-reference/data-types/string.md)) - 表中指定的主键表达式。

-   `sampling_key` ([String](../../sql-reference/data-types/string.md)) - 表中指定的采样键表达式。

-   `storage_policy` ([String](../../sql-reference/data-types/string.md)) - 存储策略:

    -   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distributed](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - 总行数，如果无法快速确定表中的确切行数，则行数返回为 `NULL` (包括底层 `Buffer` 表) 。

-   `total_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - 总字节数，如果无法快速确定存储表的确切字节数，则字节数返回为 `NULL` ( **不** 包括任何底层存储) 。

    -   如果表将数据存在磁盘上，返回实际使用的磁盘空间 (压缩后) 。
    -   如果表在内存中存储数据，返回在内存中使用的近似字节数。

-   `lifetime_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - 服务启动后插入的总行数(只针对 `Buffer` 表) 。


-   `lifetime_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) - 服务启动后插入的总字节数(只针对 `Buffer` 表) 。


-   `comment` ([String](../../sql-reference/data-types/string.md)) - 表的注释。

-   `has_own_data` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 标志，表示表本身是否在磁盘上存储数据，或者访问其他来源。 

`system.tables` 表被用于 `SHOW TABLES` 的查询实现中。

**示例**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
name:                       t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/store/81b/81b1c20a-b7c6-4116-a2ce-7583fb6b6736/']
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
metadata_modification_time: 2021-01-25 19:14:32
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE base.t1 (`n` UInt64) ENGINE = MergeTree ORDER BY n SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY n SETTINGS index_granularity = 8192
as_select:                  SELECT database AS table_catalog
partition_key:
sorting_key:                n
primary_key:                n
sampling_key:
storage_policy:             default
total_rows:                 1
total_bytes:                99
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0

Row 2:
──────
database:                   default
name:                       53r93yleapyears
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     MergeTree
is_temporary:               0
data_paths:                 ['/var/lib/clickhouse/data/default/53r93yleapyears/']
metadata_path:              /var/lib/clickhouse/metadata/default/53r93yleapyears.sql
metadata_modification_time: 2020-09-23 09:05:36
dependencies_database:      []
dependencies_table:         []
create_table_query:         CREATE TABLE default.`53r93yleapyears` (`id` Int8, `febdays` Int8) ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 8192
engine_full:                MergeTree ORDER BY id SETTINGS index_granularity = 8192
as_select:                  SELECT name AS catalog_name
partition_key:
sorting_key:                id
primary_key:                id
sampling_key:
storage_policy:             default
total_rows:                 2
total_bytes:                155
lifetime_rows:              ᴺᵁᴸᴸ
lifetime_bytes:             ᴺᵁᴸᴸ
comment:
has_own_data:               0
```


[原文](https://clickhouse.com/docs/zh/operations/system-tables/tables) <!--hide-->
