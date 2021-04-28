# system.parts {#system_tables-parts}

包含有关[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表各分区信息。

每行描述一个数据分区。

列:

-   `partition` (String) – 分区名称。要了解什么是分区，请参阅 [ALTER](../../sql-reference/statements/alter.md#query_language_queries_alter) 查询的描述。

    格式:

    -   `YYYYMM` 用于按月自动分区。
    -   `any_string` 手动分区。

-   `name` (`String`) – 数据分区的名称。

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — 数据分区的存储格式。

    可能的值：

    -   `Wide` — 每列存储在文件系统中的单独文件中。
    -   `Compact` — 所有列都存储在文件系统中的一个文件中。

    数据存储格式由[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表的 `min_bytes_for_wide_part` 和 `min_rows_for_wide_part` 设置控制。

-   `active` (`UInt8`) – 指示数据分区是否处于活动状态的标志。如果数据分区处于活动状态，则会在表中使用它。否则，将其删除。合并后，不活动的数据分区仍然保留。

-   `marks` (`UInt64`) – 标记数。要获得数据部分中大约的行数，请乘以 `marks` 索引粒度（通常为8192）（此提示不适用于自适应粒度）。

-   `rows` (`UInt64`) – 行数。

-   `bytes_on_disk` (`UInt64`) – T所有数据分区文件的总大小（以字节为单位）。

-   `data_compressed_bytes` (`UInt64`) – 数据分区中压缩数据的总大小。不包括所有辅助文件（例如，带标记的文件）。

-   `data_uncompressed_bytes` (`UInt64`) – 数据部分中未压缩数据的总大小。不包括所有辅助文件（例如，带标记的文件）。

-   `marks_bytes` (`UInt64`) – 带标记的文件的大小。

-   `modification_time` (`DateTime`) – 包含数据分区的目录被修改的时间。这通常对应于数据分区创建的时间。

-   `remove_time` (`DateTime`) – 数据分区变为非活动状态的时间。

-   `refcount` (`UInt32`) – 数据分区被使用的数。大于2的值表示在查询或合并中使用了数据分区。

-   `min_date` (`Date`) – 数据分区中日期键的最小值。

-   `max_date` (`Date`) – 数据分区中日期键的最大值。

-   `min_time` (`DateTime`) – 数据分区中日期时间键的最小值。

-   `max_time`(`DateTime`) – 数据分区中日期时间键的最大值。

-   `partition_id` (`String`) – 分区的ID。

-   `min_block_number` (`UInt64`) – 合并后组成当前分区的数据分区的最小数量。

-   `max_block_number` (`UInt64`) – 合并后组成当前分区的数据分区的最大数量。

-   `level` (`UInt32`) – 合并树的深度。0表示当前分区是通过插入而不是通过合并其他分区来创建的。

-   `data_version` (`UInt64`) – 用于确定应将哪些突变应用于数据分区（版本高于 `data_version` 的突变）的编号。

-   `primary_key_bytes_in_memory` (`UInt64`) – 主键值使用的内存量（以字节为单位）。

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – 为主键值保留的内存量（以字节为单位）。

-   `is_frozen` (`UInt8`) – 显示分区数据备份存在的标志。1，备份存在。0，备份不存在。有关更多详细信息，请参见[FREEZE PARTITION](../../sql-reference/statements/alter.md#alter_freeze-partition)。

-   `database` (`String`) – 数据库的名称。

-   `table` (`String`) – 表的名称。

-   `engine` (`String`) – 不带参数的表引擎的名称。

-   `path` (`String`) – 包含数据分区文件的文件夹的绝对路径。

-   `disk` (`String`) – 存储数据分区的磁盘的名称。

-   `hash_of_all_files` (`String`) –  压缩文件的[sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)。

-   `hash_of_uncompressed_files` (`String`) –  未压缩的文件（带标记的文件，索引文件等。)的[sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)。

-   `uncompressed_hash_of_compressed_files` (`String`) –  压缩文件中的数据的[sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)，就好像它们是未压缩的一样。

-   `delete_ttl_info_min` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的日期时间键的最小值。

-   `delete_ttl_info_max` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的日期时间键的最大值。

-   `move_ttl_info.expression` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 表达式数组。每个表达式定义一个 [TTL MOVE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)。

    !!! note "警告"
        该 `move_ttl_info.expression` 数组主要是为保持向下兼容性， 现在检查 `TTL MOVE` 规则的最简单的办法是使用 `move_ttl_info.min` 和 `move_ttl_info.max` 字段。

-   `move_ttl_info.min` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日期时间值的数组。 每个元素都描述了 [TTL MOVE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)的最小键值。

-   `move_ttl_info.max` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日期和时间值的数组。每个元素都描述了 [TTL MOVE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)的最大键值。

-   `bytes` (`UInt64`) – `bytes_on_disk` 的别名。

-   `marks_size` (`UInt64`) – `marks_bytes` 的别名。


**示例**

``` sql
SELECT * FROM system.parts LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_4_1_6
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  6
bytes_on_disk:                         310
data_compressed_bytes:                 157
data_uncompressed_bytes:               91
marks_bytes:                           144
modification_time:                     2020-06-18 13:01:49
remove_time:                           1970-01-01 00:00:00
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
min_time:                              1970-01-01 00:00:00
max_time:                              1970-01-01 00:00:00
partition_id:                          all
min_block_number:                      1
max_block_number:                      4
level:                                 1
data_version:                          6
primary_key_bytes_in_memory:           8
primary_key_bytes_in_memory_allocated: 64
is_frozen:                             0
database:                              default
table:                                 months
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/months/all_1_4_1_6/
hash_of_all_files:                     2d0657a16d9430824d35e327fcbd87bf
hash_of_uncompressed_files:            84950cc30ba867c77a408ae21332ba29
uncompressed_hash_of_compressed_files: 1ad78f1c6843bbfb99a2c931abe7df7d
delete_ttl_info_min:                   1970-01-01 00:00:00
delete_ttl_info_max:                   1970-01-01 00:00:00
move_ttl_info.expression:              []
move_ttl_info.min:                     []
move_ttl_info.max:                     []
```

**另请参阅**

-   [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)
-   [TTL for Columns and Tables](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/parts) <!--hide-->