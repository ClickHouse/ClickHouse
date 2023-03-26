# system.parts_columns {#system_tables-parts_columns}

包含关于[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)表的部分和列的信息.

每一行描述一个数据部分.

列信息:

-   `partition` ([String](../../sql-reference/data-types/string.md)) — 分区的名称. 要了解什么是分区, 请参阅[ALTER](../../sql-reference/statements/alter/index.md#query_language_queries_alter)查询的描述.

    格式:

    -   `YYYYMM` 按月自动分区.
    -   `any_string` 当手动分区.

-   `name` ([String](../../sql-reference/data-types/string.md)) — 数据部分的名称.

-   `part_type` ([String](../../sql-reference/data-types/string.md)) — 数据部分存储格式.

    可能的值:

    -   `Wide` — 每一列存储在文件系统中的一个单独的文件中.
    -   `Compact` — 所有列都存储在文件系统中的一个文件中.
    
    数据存储格式由[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)表的 `min_bytes_for_wide_part` 和 `min_rows_for_wide_part` 设置控制.
    
-   `active` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 数据部分是否处于活动状态的标志. 如果数据部分是活动的, 则在表中使用它. 否则, 它被删除. 合并后仍保留非活动数据部分.

-   `marks` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 标记数. 要获得数据部分中的大约行数, 请将“标记”乘以索引粒度(通常为8192)(此提示不适用于自适应粒度).

-   `rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 行数.

-   `bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 所有数据部分文件的总大小(以字节为单位).

-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 在数据部分中压缩数据的总大小. 不包括所有辅助文件(例如，带有标记的文件).

-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 数据部分中未压缩数据的总大小. 不包括所有辅助文件(例如，带有标记的文件).

-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 带标记的文件的大小.

-   `modification_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 包含数据部分的目录被修改的时间. 这通常对应于数据部分创建的时间.

-   `remove_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — 数据部分变为非活动状态的时间.

-   `refcount` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 使用数据部分的位置数. 大于2的值表示该数据部分用于查询或合并.

-   `min_date` ([Date](../../sql-reference/data-types/date.md)) — 数据部分中日期键的最小值.

-   `max_date` ([Date](../../sql-reference/data-types/date.md)) — 数据部分中日期键的最大值.

-   `partition_id` ([String](../../sql-reference/data-types/string.md)) — 分区ID.

-   `min_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 合并后组成当前部分的数据部分最小值.

-   `max_block_number` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 合并后组成当前部分的数据部分最大值.

-   `level` ([UInt32](../../sql-reference/data-types/int-uint.md)) — 合并树的深度. 0表示当前部分是通过插入而不是合并其他部分创建的.

-   `data_version` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 用于确定应该对数据部分应用哪些突变的编号(版本高于 `data_version` 的突变).

-   `primary_key_bytes_in_memory` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 主键值使用的内存量(以字节为单位).

-   `primary_key_bytes_in_memory_allocated` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 为主键值保留的内存量(以字节为单位).

-   `database` ([String](../../sql-reference/data-types/string.md)) — 数据库名称.

-   `table` ([String](../../sql-reference/data-types/string.md)) — 表名称.

-   `engine` ([String](../../sql-reference/data-types/string.md)) — 不带参数的表引擎的名称.

-   `disk_name` ([String](../../sql-reference/data-types/string.md)) — 存储数据部分的磁盘名称.

-   `path` ([String](../../sql-reference/data-types/string.md)) — 数据部件文件文件夹的绝对路径.

-   `column` ([String](../../sql-reference/data-types/string.md)) — 列名称.

-   `type` ([String](../../sql-reference/data-types/string.md)) — 列类型.

-   `column_position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 表中以1开头的一列的序号位置.

-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — 默认值的表达式类型 (`DEFAULT`, `MATERIALIZED`, `ALIAS`), 如果未定义则为空字符串.

-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — 表达式的默认值, 如果未定义则为空字符串.

-   `column_bytes_on_disk` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 列的总大小(以字节为单位).

-   `column_data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 列中压缩数据的总大小，以字节为单位.

-   `column_data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 列中解压缩数据的总大小，以字节为单位.

-   `column_marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 带标记的列的大小，以字节为单位.

-   `bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `bytes_on_disk` 别名.

-   `marks_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — `marks_bytes` 别名.

**示例**

``` sql
SELECT * FROM system.parts_columns LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
partition:                             tuple()
name:                                  all_1_2_1
part_type:                             Wide
active:                                1
marks:                                 2
rows:                                  2
bytes_on_disk:                         155
data_compressed_bytes:                 56
data_uncompressed_bytes:               4
marks_bytes:                           96
modification_time:                     2020-09-23 10:13:36
remove_time:                           2106-02-07 06:28:15
refcount:                              1
min_date:                              1970-01-01
max_date:                              1970-01-01
partition_id:                          all
min_block_number:                      1
max_block_number:                      2
level:                                 1
data_version:                          1
primary_key_bytes_in_memory:           2
primary_key_bytes_in_memory_allocated: 64
database:                              default
table:                                 53r93yleapyears
engine:                                MergeTree
disk_name:                             default
path:                                  /var/lib/clickhouse/data/default/53r93yleapyears/all_1_2_1/
column:                                id
type:                                  Int8
column_position:                       1
default_kind:
default_expression:
column_bytes_on_disk:                  76
column_data_compressed_bytes:          28
column_data_uncompressed_bytes:        2
column_marks_bytes:                    48
```

**另请参阅**

-   [MergeTree family](../../engines/table-engines/mergetree-family/mergetree.md)

[原始文章](https://clickhouse.com/docs/en/operations/system_tables/parts_columns) <!--hide-->
