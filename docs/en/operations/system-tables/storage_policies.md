---
slug: /en/operations/system-tables/storage_policies
---
# storage_policies

Contains information about storage policies and volumes defined in the [server configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

- `policy_name` ([String](../../sql-reference/data-types/string.md)) — Name of the storage policy.
- `volume_name` ([String](../../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
- `volume_priority` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration, the data fills the volumes according this priority, i.e. data during inserts and merges is written to volumes with a lower priority (taking into account other rules: TTL, `max_data_part_size`, `move_factor`).
- `disks` ([Array(String)](../../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
- `volume_type` ([Enum8](../../sql-reference/data-types/enum.md))  — Type of volume. Can have one of the following values:
    - `JBOD` 
    - `SINGLE_DISK`
    - `UNKNOWN`
- `max_data_part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
- `move_factor` ([Float64](../../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.
- `prefer_not_to_merge` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Value of the `prefer_not_to_merge` setting. When this setting is enabled, merging data on this volume is not allowed. This allows controlling how ClickHouse works with slow disks.
- `perform_ttl_move_on_insert` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Value of the `perform_ttl_move_on_insert` setting. — Disables TTL move on data part INSERT. By default if we insert a data part that already expired by the TTL move rule it immediately goes to a volume/disk declared in move rule. This can significantly slowdown insert in case if destination volume/disk is slow (e.g. S3).
- `load_balancing` ([Enum8](../../sql-reference/data-types/enum.md))  — Policy for disk balancing. Can have one of the following values:
    - `ROUND_ROBIN`
    - `LEAST_USED`

If the storage policy contains more then one volume, then information for each volume is stored in the individual row of the table.
