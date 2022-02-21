# system.mutations {#system_tables-mutations}

该表包含关于MergeTree表的[突变](../../sql-reference/statements/alter.md#alter-mutations)及其进展的信息 。每条突变命令都用一行来表示。

该表具有以下列属性:

-   `database` ([String](../../sql-reference/data-types/string.md)) — 应用突变的数据库名称。

-   `table` ([String](../../sql-reference/data-types/string.md)) — 应用突变的表名称。

-   `mutation_id` ([String](../../sql-reference/data-types/string.md)) — 突变的ID。对于复制表，这些ID对应于ZooKeeper中<table_path_in_zookeeper>/mutations/目录下的znode名称。对于非复制表，ID对应表的数据目录中的文件名。

-   `command` ([String](../../sql-reference/data-types/string.md)) — 突变命令字符串（`ALTER TABLE [db.]table`语句之后的部分)。

-   `create_time` ([Datetime](../../sql-reference/data-types/datetime.md)) — 突变命令提交执行的日期和时间。

-   `block_numbers.partition_id` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 对于复制表的突变，该数组包含分区的ID（每个分区都有一条记录）。对于非复制表的突变，该数组为空。

-   `block_numbers.number` ([Array](../../sql-reference/data-types/array.md)([Int64](../../sql-reference/data-types/int-uint.md))) — 对于复制表的突变，该数组包含每个分区的一条记录，以及通过突变获取的块号。只有包含块号小于该数字的块的part才会在分区中应用突变。
  
    在非复制表中，所有分区中的块号组成一个序列。这意味着对于非复制表的突变，该列将包含一条记录，该记录具有通过突变获得的单个块号。
    
-   `parts_to_do_names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — 由需要应用突变的part名称构成的数组。

-   `parts_to_do` ([Int64](../../sql-reference/data-types/int-uint.md)) — 需要应用突变的part的数量。

-   `is_done` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 突变是否完成的标志。其中：
    -   1，表示突变已经完成。
    -   0，表示突变仍在进行中。


!!! info "注意"
    即使 parts_to_do = 0，由于长时间运行的`INSERT`查询将创建需要突变的新part，也可能导致复制表突变尚未完成。

如果某些parts在突变时出现问题，以下列将包含附加信息：

-   `latest_failed_part`([String](../../sql-reference/data-types/string.md)) — 最近不能突变的part的名称。

-   `latest_fail_time`([Datetime](../../sql-reference/data-types/datetime.md)) — 最近的一个突变失败的时间。

-   `latest_fail_reason`([String](../../sql-reference/data-types/string.md)) — 导致最近part的突变失败的异常消息。


**另请参阅**

- Mutations
- [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 表引擎
- [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 家族

[Original article](https://clickhouse.com/docs/en/operations/system_tables/mutations) <!--hide-->