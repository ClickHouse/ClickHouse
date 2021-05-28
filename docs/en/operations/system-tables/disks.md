# system.disks {#system_tables-disks}

Contains information about disks defined in the [server configuration](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([String](../../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` parameter of disk configuration.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/disks) <!--hide-->
