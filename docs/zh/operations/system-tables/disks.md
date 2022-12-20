# system.disks {#system_tables-disks}

包含在 [服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure) 中定义的磁盘信息.

列:

-   `name` ([字符串](../../sql-reference/data-types/string.md)) — 服务器配置中的磁盘名称.
-   `path` ([字符串](../../sql-reference/data-types/string.md)) — 文件系统中挂载点的路径.
-   `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 磁盘上的可用空间，以字节为单位.
-   `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 磁盘容量，以字节为单位。
-   `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 在磁盘上应保持空闲的磁盘空间的数量，以字节为单位。在磁盘配置的 `keep_free_space_bytes` 参数中定义。

**示例**

```sql
:) SELECT * FROM system.disks;
```

```text
┌─name────┬─path─────────────────┬───free_space─┬──total_space─┬─keep_free_space─┐
│ default │ /var/lib/clickhouse/ │ 276392587264 │ 490652508160 │               0 │
└─────────┴──────────────────────┴──────────────┴──────────────┴─────────────────┘

1 rows in set. Elapsed: 0.001 sec.
```

[原文](https://clickhouse.com/docs/zh/operations/system-tables/disks) <!--hide-->
