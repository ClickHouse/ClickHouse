# system.disks {#system_tables-disks}

包含有关[服务器配置](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure)中定义的磁盘的信息。

列:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 服务器配置中的磁盘名称。
-   `path` ([String](../../sql-reference/data-types/string.md)) — 文件系统中装载的路径。
-   `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 磁盘上的可用空间（以字节为单位）。
-   `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 磁盘卷，以字节为单位。
-   `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 在磁盘上保持可用空间的磁盘空间量（以字节为单位）。在 `keep_free_space_bytes` 磁盘配置参数中定义。

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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/disks) <!--hide-->