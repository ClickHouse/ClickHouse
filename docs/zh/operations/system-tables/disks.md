---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。磁盘 {#system_tables-disks}

包含有关在定义的磁盘信息 [服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `name` ([字符串](../../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([字符串](../../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` 磁盘配置参数。

## 系统。storage_policies {#system_tables-storage_policies}

包含有关存储策略和卷中定义的信息 [服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

列:

-   `policy_name` ([字符串](../../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([字符串](../../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([数组（字符串)](../../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

如果存储策略包含多个卷，则每个卷的信息将存储在表的单独行中。
