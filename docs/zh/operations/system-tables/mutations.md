# system.mutations {#system_tables-mutations}

该表包含MergeTree表的[突变](../../sql-reference/statements/alter.md#alter-mutations) 及其进程的信息。 每个突变命令由一行表示。

列:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 应用该突变的数据库的名称。

-   `table` ([String](../../sql-reference/data-types/string.md)) — 应用该突变的表的名称。

-   `mutation_id` ([String](../../sql-reference/data-types/string.md)) —  突变的ID。对于复制表，这些ID对应于ZooKeeper中目录 `<table_path_in_zookeeper>/mutations/` 中的znode名称。对于非复制表，ID对应于表的数据目录中的文件名。

-   `command` ([String](../../sql-reference/data-types/string.md)) — 突变命令 (`ALTER TABLE [db.]table`后面的语句部分)。

-   `create_time` ([Datetime](../../sql-reference/data-types/datetime.md)) —  突变命令提交执行的日期和时间。

-   `block_numbers.partition_id` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 对于复制表的突变，该数组包含分区的ID（每个分区一个记录）。对于非复制表的突变，数组为空。

-   `block_numbers.number` ([Array](../../sql-reference/data-types/array.md)([Int64](../../sql-reference/data-types/int-uint.md))) — 对于复制表的突变，该阵列为每个分区包含一个记录，并包含该突变获取的块号。只有包含编号小于此编号的块的零件才会在分区中发生突变。
    
    在非复制表中，所有分区中的块号形成单个序列。这意味着对于非复制表的突变，该列将包含一个记录，该记录具有通过突变获取的单个块号。

-   `parts_to_do_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 要完成突变，需要突变的数据分区的名称的数组。

-   `parts_to_do` ([Int64](../../sql-reference/data-types/int-uint.md)) — 要完成突变，需要突变的数据分区的数量。

-   `is_done` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 标记是否完成突变的标志。可能的值：
    -   `1` 如果突变完成，
    -   `0` 如果突变仍在进行中。

!!! info "注意"
    即使 `parts_to_do = 0` ，由于长时间运行的 `INSERT` 将创建需要突变的新数据分区，因此可能尚未完成复制表的突变。 

如果在改变某些分区时出现问题，则以下列将包含额外的信息：

-   `latest_failed_part` ([String](../../sql-reference/data-types/string.md)) — 最近不能突变的分区的名称。 

-   `latest_fail_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — 最近分区的突变失败的时间。

-   `latest_fail_reason` ([String](../../sql-reference/data-types/string.md)) — 导致最近分区的突变失败的异常消息。

**另请参阅**

-   Mutations
-   [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表引擎
-   [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 家族

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/mutations) <!--hide-->
