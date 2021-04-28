# system.part_log {#system_tables-part-log}

该 `system.part_log` 表只有当服务器设置指定了 [part_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-part-log) 时，才创建该表。

此表包含有关[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族表的 [数据分区](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 发生的事件的信息，例如添加或合并数据。

该 `system.part_log` 表包含以下列:

-   `query_id` ([String](../../sql-reference/data-types/string.md)) — 创建此数据分区的 `INSERT` 查询ID。
-   `event_type` (Enum) — 数据分区发生的事件的类型。可以是以下其中一个值：
    -   `NEW_PART` — 插入新的数据分区。
    -   `MERGE_PARTS` — 合并数据分区。
    -   `DOWNLOAD_PART` — 下载数据分区。
    -   `REMOVE_PART` — 使用 [DETACH PARTITION](../../sql-reference/statements/alter.md#alter_detach-partition)删除或分离数据分区。
    -   `MUTATE_PART` — 更改数据分区。
    -   `MOVE_PART` — 将数据部分从一个磁盘移动到另一磁盘。
-   `event_date` (Date) — 事件日期。
-   `event_time` (DateTime) — 事件时间。
-   `event_time_microseconds` ([DateTime64](../../sql-reference/data-types/datetime64.md)) — 事件时间，以微秒为单位。
-   `duration_ms` (UInt64) — 持续时间。
-   `database` (String) — 数据分区所在的数据库的名称。
-   `table` (String) — 数据分区所在的表的名称。
-   `part_name` (String) — 数据分区的名称。
-   `partition_id` (String) — 数据分区插入到的分区的ID。如果分区是通过 `tuple()` ，则该列将采用 `all` 值。
-   `rows` (UInt64) — 数据分区中的行数。
-   `size_in_bytes` (UInt64) — 数据分区的大小，以字节为单位。
-   `merged_from` (Array(String)) — 由组成当前分区（合并后）的分区的名称组成的数组。
-   `bytes_uncompressed` (UInt64) — 未压缩字节的大小。
-   `read_rows` (UInt64) — 合并期间读取的行数。
-   `read_bytes` (UInt64) — 合并期间读取的字节数。
-   `peak_memory_usage` ([Int64](../../sql-reference/data-types/int-uint.md)) — 在此线程的上下文中分配的内存和已释放的内存之间的最大差异。
-   `error` (UInt16) — 发生的错误代码号。
-   `exception` (String) — 发生的错误信息。

该 `system.part_log` 表是在第一次向`MergeTree` 表中插入数据之后创建的。

**示例**

``` sql
SELECT * FROM system.part_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
query_id:                      983ad9c7-28d5-4ae1-844e-603116b7de31
event_type:                    NewPart
event_date:                    2021-02-02
event_time:                    2021-02-02 11:14:28
event_time_microseconds:                    2021-02-02 11:14:28.861919
duration_ms:                   35
database:                      default
table:                         log_mt_2
part_name:                     all_1_1_0
partition_id:                  all
path_on_disk:                  db/data/default/log_mt_2/all_1_1_0/
rows:                          115418
size_in_bytes:                 1074311
merged_from:                   []
bytes_uncompressed:            0
read_rows:                     0
read_bytes:                    0
peak_memory_usage:             0
error:                         0
exception:                   
```

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/part_log) <!--hide-->
