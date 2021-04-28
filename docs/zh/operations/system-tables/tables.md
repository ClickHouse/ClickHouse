# system.tables {#system-tables}

包含服务器知道的每个表的元数据。

分离(detached) 的表不显示在 `system.tables`中。

临时表 (temporay tables) 仅在创建它们的那些会话中可见。它们显示为空 `database` 字段，并且 `is_temporary` 标记已打开。

列:

-   `database` (String) — 表所在的数据库的名称。

-   `name` (String) — 表名。

-   `engine` (String) — 表引擎名称（不带参数）。

-   `is_temporary` (UInt8)-指示表是否为临时的标志。

-   `data_path` (String)-文件系统中表数据的路径。

-   `metadata_path` (String)-文件系统中表元数据的路径。

-   `metadata_modification_time` (DateTime)-表元数据的最新修改时间。

-   `dependencies_database` (Array(String))-数据库依赖关系.

-   `dependencies_table` （Array(String)）-表依赖关系 (基于当前表的 [MaterializedView](../../engines/table-engines/special/materializedview.md) 表）。

-   `create_table_query` (String)-用于创建表的查询。

-   `engine_full` (String)-表引擎的参数。

-   `partition_key` (String)-表中指定的分区键表达式。

-   `sorting_key` (String)-表中指定的排序键表达式。

-   `primary_key` (String)-表中指定的主键表达式。

-   `sampling_key` (String)-表中指定的采样键表达式。

-   `storage_policy` (String)-存储策略:

    -   [合并树](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [分布式](../../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64))-总行数，如果可以快速确定表中的确切行数，否则 `Null` （包括底层 `Buffer` 表）。

-   `total_bytes` (Nullable(UInt64))-总字节数，如果可以快速确定存储表的确切字节数，否则 `Null` (**不** 包括任何底层存储）。

    -   如果表在磁盘上存储数据，则返回磁盘上已使用的空间（即压缩的空间）。
    -   如果表在内存中存储数据，则返回在内存中使用的近似字节数.
-   `lifetime_rows` (Nullable(UInt64)) - 自服务器启动以来已插入的总行数（仅适用于 `Buffer` 表）。

-   `lifetime_bytes` (Nullable(UInt64)) - 自服务器启动以来已插入的总字节数（仅适用于 `Buffer` 表）。

该 `system.tables` 表用于 `SHOW TABLES` 查询实现。

**示例**

```sql
SELECT * FROM system.tables LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                   system
name:                       aggregate_function_combinators
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAggregateFunctionCombinators
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/aggregate_function_combinators.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ

Row 2:
──────
database:                   system
name:                       asynchronous_metrics
uuid:                       00000000-0000-0000-0000-000000000000
engine:                     SystemAsynchronousMetrics
is_temporary:               0
data_paths:                 []
metadata_path:              /var/lib/clickhouse/metadata/system/asynchronous_metrics.sql
metadata_modification_time: 1970-01-01 03:00:00
dependencies_database:      []
dependencies_table:         []
create_table_query:         
engine_full:                
partition_key:              
sorting_key:                
primary_key:                
sampling_key:               
storage_policy:             
total_rows:                 ᴺᵁᴸᴸ
total_bytes:                ᴺᵁᴸᴸ
```

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/tables) <!--hide-->