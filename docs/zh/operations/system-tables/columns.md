# system.columns {#system-columns}

包含有关所有表中列的信息。

您可以使用此表来获取与[DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table) 查询类似的信息，但是一次可以获取多个表。

临时表中的列仅在创建它们的那些会话中在 `system.columns` 可见。 它们以空的 `database` 字段显示。

列:

-   `database` (String) — 数据库名称。
-   `table` (String) — 表名。
-   `name` (String) — 列名。
-   `type` (String) — 列类型。
-   `position` (UInt64) — 表中列的顺序位置，以1开头。
-   `default_kind` (String) — 默认值的表达式类型 (`DEFAULT`, `MATERIALIZED`, `ALIAS`)，如果没有定义，则为空字符串。
-   `default_expression` (String) — 默认值的表达式，如果没有定义，则为空字符串。
-   `data_compressed_bytes` (UInt64) — 压缩数据的大小，以字节为单位。
-   `data_uncompressed_bytes` (UInt64) — T解压缩数据的大小，以字节为单位。
-   `marks_bytes` (UInt64) — 标记的大小，以字节为单位。
-   `comment` (String) — 列上的注释，如果没有定义，则为空字符串。
-   `is_in_partition_key` (UInt8) — 指示列是否在分区表达式中的标志。
-   `is_in_sorting_key` (UInt8) — 指示列是否在排序键表达式中的标志。
-   `is_in_primary_key` (UInt8) — 指示列是否在主键表达式中的标志。
-   `is_in_sampling_key` (UInt8) — 指示列是否在采样关键字表达式中的标志。
-   `compression_codec`（String）— 压缩编解码器名称。

**示例**

```sql
SELECT * FROM system.columns LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                system
table:                   aggregate_function_combinators
name:                    name
type:                    String
default_kind:            
default_expression:      
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:                 
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:       

Row 2:
──────
database:                system
table:                   aggregate_function_combinators
name:                    is_internal
type:                    UInt8
default_kind:            
default_expression:      
data_compressed_bytes:   0
data_uncompressed_bytes: 0
marks_bytes:             0
comment:                 
is_in_partition_key:     0
is_in_sorting_key:       0
is_in_primary_key:       0
is_in_sampling_key:      0
compression_codec:       
```

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/columns) <!--hide-->
