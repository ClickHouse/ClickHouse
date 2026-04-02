---
description: 'System table containing information about data files stored on remote
  disks such as S3 or Azure Blob Storage.'
keywords: ['system table', 'remote_data_paths']
slug: /operations/system-tables/remote_data_paths
title: 'system.remote_data_paths'
doc_type: 'reference'
---

Contains information about data files stored on remote disks (e.g. S3, Azure Blob Storage), including the mapping between local metadata paths and remote blob paths.

Each row represents one remote blob object associated with a data file.

Columns:

- `disk_name` ([String](../../sql-reference/data-types/string.md)) — Name of the remote disk as defined in the storage configuration.
- `path` ([String](../../sql-reference/data-types/string.md)) — Root path of the remote disk as configured in storage configuration.
- `cache_base_path` ([String](../../sql-reference/data-types/string.md)) — Base directory for cache files associated with the remote disk.
- `local_path` ([String](../../sql-reference/data-types/string.md)) — Local metadata file path relative to the ClickHouse data directory, pointing to the file that maps to the remote blob.
- `remote_path` ([String](../../sql-reference/data-types/string.md)) — Blob path in the remote object storage that the local metadata file maps to.
- `size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Compressed size of the file in bytes.
- `common_prefix_for_blobs` ([String](../../sql-reference/data-types/string.md)) — Common prefix for blobs in the remote object storage, applicable when multiple blobs share a path prefix.
- `cache_paths` ([Array(String)](../../sql-reference/data-types/array.md)) — Local cache file paths corresponding to the remote blob.

**Settings**

- [`traverse_shadow_remote_data_paths`](../../operations/settings/settings.md#traverse_shadow_remote_data_paths) — When enabled, the table also includes data from frozen partitions (the `shadow/` directory used by `ALTER TABLE ... FREEZE`). Disabled by default.

**Example**

```sql
SELECT * FROM system.remote_data_paths LIMIT 1 FORMAT Vertical;
```

```text
Row 1:
──────
disk_name:              s3
path:                   /var/lib/clickhouse/disks/s3/
cache_base_path:        /var/lib/clickhouse/disks/s3_cache/
local_path:             store/123/1234abcd-1234-1234-1234-1234abcd1234/all_0_0_0/data.bin
remote_path:            abc123/all_0_0_0/data.bin
size:                   1048576
common_prefix_for_blobs:
cache_paths:            ['/var/lib/clickhouse/disks/s3_cache/a1/b2/c3d4e5f6']
```

**See Also**

- [Using external storage for data storage](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-s3)
- [Configuring external storage](/operations/storing-data.md/#configuring-external-storage)
- [system.disks](../../operations/system-tables/disks.md)
