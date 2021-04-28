# system.merges {#system-merges}

包含有关MergeTree系列中表当前正在进行的合并和分区突变(part mutation)的信息。

列:

-   `database` (String) — 表所在的数据库的名称。
-   `table` (String) — 表名。
-   `elapsed` (Float64) — 自合并开始起经过的时间（以秒为单位）。
-   `progress` (Float64) — 从0到1的已完成工作的百分比。 
-   `num_parts` (UInt64) — 要合并的片段数。
-   `result_part_name` (String) — 合并结果将形成的分区(part)的名称。
-   `is_mutation` (UInt8)- 如果这个过程是一个分区突变，则为1。
-   `total_size_bytes_compressed` (UInt64) — 合并块中压缩数据的总大小。
-   `total_size_marks` (UInt64) — 合并分区中的标记总数。
-   `bytes_read_uncompressed` (UInt64) — 读取的未压缩字节数。
-   `rows_read` (UInt64) — 读取的行数。
-   `bytes_written_uncompressed` (UInt64) — 写入的未压缩字节数。
-   `rows_written` (UInt64) — 写入的行数。
-   `memory_usage` (UInt64) — 合并过程的内存消耗。
-   `thread_id` (UInt64) — 合并过程的线程ID。
-   `merge_type` — 当前合并的类型。如果是突变则为空。
-   `merge_algorithm` — 当前合并中使用的算法。如果是突变则为空。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/merges) <!--hide-->
