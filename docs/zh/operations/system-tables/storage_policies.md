# system.storage_policies {#system_tables-storage_policies}

包含有关[服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure)中定义的存储策略和卷的信息。

列:

-   `policy_name` ([String](../../sql-reference/data-types/string.md)) — 存储策略的名称。
-   `volume_name` ([String](../../sql-reference/data-types/string.md)) — 在存储策略中定义的卷名称。
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 配置中的卷订单号。 
-   `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — 磁盘名称，在存储策略中定义。
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 可以存储在卷磁盘上的数据部分的最大大小（0-无限制）。
-   `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — 可用磁盘空间比率。当比率超过配置参数的值时，ClickHouse开始按顺序将数据移至下一个卷。
-   `prefer_not_to_merge` ([UInt8](../../sql-reference/data-types/int-uint.md)) —  `prefer_not_to_merge` 设置的值。启用此设置后，将无法合并该卷上的数据。这样可以控制ClickHouse如何与慢速磁盘一起使用。

如果存储策略包含多个卷，则每个卷的信息将存储在表的单独行中。

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/storage_policies) <!--hide-->
