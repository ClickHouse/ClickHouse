# system.storage\_policies {#system_tables-storage_policies}

Contains information about storage policies and volumes defined in the [server configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

-   `policy_name` ([String](../../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([String](../../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

If the storage policy contains more then one volume, then information for each volume is stored in the individual row of the table.
