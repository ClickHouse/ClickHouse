# system.parts {#system_tables-parts}

此系统表包含 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表分区的相关信息。

每一行描述一个数据分区。

列:

-   `partition` ([String](../../sql-reference/data-types/string.md)) – 分区名称。请参阅 [ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter) 查询的说明，来了解什么是分区。

    格式:

    -   `YYYYMM` 用于按月自动分区。
    -   `any_string` 手动分区时，是其他格式的字符串。

-   `name` ([String](../../sql-reference/data-types/string.md)) – 数据分区的名称。

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — 数据分区的存储格式。

    可能的值:

    -   `Wide` — 每一列在文件系统中的一个单独文件中存储。
    -   `Compact` — 所有列在文件系统中的一个文件中存储。

    数据存储格式由 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表的 `min_bytes_for_wide_part` 和 `min_rows_for_wide_part` 控制。

   -   `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) – 指示数据分区是否处于活动状态的标志。如果数据分区处于活动状态，则此数据正在被表使用。反之，则不活跃(deleted)。合并后仍会保留非活跃的数据分区。

-   `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 标记数。要获得数据分区中的大致行数：使用`marks`(标记数)乘以索引粒度(通常为 8192)。不适用于自适应颗粒度。

-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 行数.

-   `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 数据总大小（以字节为单位）。

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 数据分区中压缩数据的总大小。不包括所有辅助文件（例如，带有标记的文件）。

-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 数据分区中未压缩数据的总大小。不包括所有辅助文件（例如，带有标记的文件）。

-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 带有标记的文件的大小。

-   `secondary_indices_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 数据分区中二级索引的压缩数据总大小。所有的辅助文件（例如，带有标记的文件）都不包括在内。

-   `secondary_indices_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 数据分区中二级索引的未压缩数据的总大小。所有的辅助文件（例如，带有标记的文件）都不包括在内。

-   `secondary_indices_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 带标记的二级索引的文件大小。

-   `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – 包含数据分区的目录被修改的时间。这通常对应于数据部分创建的时间。

-   `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – 数据分区变为非活动状态的时间。

-   `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) – 使用数据部分的位置数。大于 2 的值表示数据部分用于查询或是用于合并。

-   `min_date` ([Date](../../sql-reference/data-types/date.md)) – 数据部分中日期键的最小值。

-   `max_date` ([Date](../../sql-reference/data-types/date.md)) – 数据部分中日期键的最大值。

-   `min_time` ([DateTime](../../sql-reference/data-types/datetime.md)) – 数据部分中日期和时间键的最小值。

-   `max_time`([DateTime](../../sql-reference/data-types/datetime.md)) – 数据部分中日期和时间键的最大值。

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) – 分区的 ID。

-   `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 合并后构成当前部分的最小数据部分数量。

-   `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 合并后构成当前部分的最大数据部分数量。

-   `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) – 合并树的深度。值为 0 表示该分区是通过插入创建的，而不是通过合并创建的。

-   `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 用于确定应将哪些订正(mutations)应用于数据部分（版本高于 `data_version` 的订正(mutations)）的数字。

-   `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 主键值使用的内存量（以字节为单位）。

-   `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) – 为主键值保留的内存量（以字节为单位）。

-   `is_frozen` ([UInt8](../../sql-reference/data-types/int-uint.md)) – 显示分区数据备份存在的标志。1，备份存在。0，备份不存在。更多细节，见 [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md#alter_freeze-partition)。

-   `database` ([String](../../sql-reference/data-types/string.md)) – 数据库的名称。

-   `table` ([String](../../sql-reference/data-types/string.md)) – 表的名称。

-   `engine` ([String](../../sql-reference/data-types/string.md)) – 不带参数的表引擎名称。

-   `path` ([String](../../sql-reference/data-types/string.md)) – 包含数据部分文件的文件夹的绝对路径。

-   `disk` ([String](../../sql-reference/data-types/string.md)) – 存储数据部分的磁盘的名称。

-   `hash_of_all_files` ([String](../../sql-reference/data-types/string.md)) – 压缩文件的 [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)。

-   `hash_of_uncompressed_files` ([String](../../sql-reference/data-types/string.md)) – 未压缩文件(带有标记的文件、索引文件等)的 [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)。

-   `uncompressed_hash_of_compressed_files` ([String](../../sql-reference/data-types/string.md)) – 压缩文件中的数据(没有压缩时)的 [sipHash128](../../sql-reference/functions/hash-functions.md#hash_functions-siphash128)。

-   `delete_ttl_info_min` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的日期和时间键的最小值。

-   `delete_ttl_info_max` ([DateTime](../../sql-reference/data-types/datetime.md)) — [TTL DELETE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的日期和时间键的最大值。

-   `move_ttl_info.expression` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 表达式的数组。 每个表达式定义一个 [TTL MOVE 规则](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

    !!! note "警告"
        保留 `move_ttl_info.expression` 数组主要是为了向后兼容，现在检查 `TTL MOVE` 规则最简单的方法是使用 `move_ttl_info.min` 和 `move_ttl_info.max` 字段。

-   `move_ttl_info.min` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日期值和时间值的数组。数组中的每个元素都描述了一个 [TTL MOVE rule](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的最小键值。

-   `move_ttl_info.max` ([Array](../../sql-reference/data-types/array.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 日期值和时间值的数组。数组中的每个元素都描述了一个 [TTL MOVE rule](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) 的最大键值。

-   `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) – `bytes_on_disk`的别名。

-   `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) – `marks_bytes`的别名。

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
secondary_indices_compressed_bytes:    58
secondary_indices_uncompressed_bytes:  6
secondary_indices_marks_bytes:         48
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

-   [MergeTree(合并树)家族](../../engines/table-engines/mergetree-family/mergetree.md)
-   [列和表的 TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)

[原文](https://clickhouse.com/docs/zh/operations/system-tables/parts) <!--hide-->
