# system.storage_policies {#system_tables-storage_policies}

包含有关 [服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure) 中定义的存储策略和卷信息。

列:

-   `policy_name` ([String](../../sql-reference/data-types/string.md)) — 存储策略的名称。
-   `volume_name` ([String](../../sql-reference/data-types/string.md)) — 存储策略中定义的卷名称。
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 配置中的卷顺序号，数据根据这个优先级填充卷，比如插入和合并期间的数据将被写入优先级较低的卷 (还需考虑其他规则: TTL, `max_data_part_size`, `move_factor`)。
-   `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — 存储策略中定义的磁盘名。
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 可以存储在卷磁盘上数据部分的最大大小 (0 - 不限制)。
-   `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — 磁盘空闲的比率。当比率超过配置的值，ClickHouse 将把数据向下一个卷移动。
-   `prefer_not_to_merge` ([UInt8](../../sql-reference/data-types/int-uint.md)) — 设置中 `prefer_not_to_merge` 的值. 当这个设置启用时，不允许在此卷上合并数据。这将允许控制 ClickHouse 如何与运行速度较慢的磁盘一起工作。

如果存储策略包含多个卷，则每个卷的信息将在表中作为单独一行存储。

[原文](https://clickhouse.com/docs/zh/operations/system-tables/storage_policies) <!--hide-->
