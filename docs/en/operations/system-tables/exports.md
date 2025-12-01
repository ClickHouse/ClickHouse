---
description: 'System table containing information about in progress merge tree part exports'
keywords: ['system table', 'exports', 'merge tree', 'part']
slug: /operations/system-tables/exports
title: 'system.exports'
---

Contains information about in progress merge tree part exports

Columns:

- `source_database` ([String](/docs/en/sql-reference/data-types/string.md)) — Name of the source database.
- `source_table` ([String](/docs/en/sql-reference/data-types/string.md)) — Name of the source table.
- `destination_database` ([String](/docs/en/sql-reference/data-types/string.md)) — Name of the destination database.
- `destination_table` ([String](/docs/en/sql-reference/data-types/string.md)) — Name of the destination table.
- `create_time` ([DateTime](/docs/en/sql-reference/data-types/datetime.md)) — Date and time when the export command was received in the server.
- `part_name` ([String](/docs/en/sql-reference/data-types/string.md)) — Name of the part.
- `destination_file_path` ([String](/docs/en/sql-reference/data-types/string.md)) — File path relative to where the part is being exported to.
- `elapsed` ([Float64](/docs/en/sql-reference/data-types/float.md)) — The time elapsed (in seconds) since the export started.
- `rows_read` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — The number of rows read from the exported part.
- `total_rows_to_read` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — The total number of rows to read from the exported part.
- `total_size_bytes_compressed` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — The total size of the compressed data in the exported part.
- `total_size_bytes_uncompressed` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — The total size of the uncompressed data in the exported part.
- `bytes_read_uncompressed` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — The number of uncompressed bytes read from the exported part.
- `memory_usage` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — Current memory usage in bytes for the export operation.
- `peak_memory_usage` ([UInt64](/docs/en/sql-reference/data-types/int-uint.md)) — Peak memory usage in bytes during the export operation.

**Example**

```sql
arthur :) select * from system.exports;

SELECT *
FROM system.exports

Query id: 2026718c-d249-4208-891b-a271f1f93407

Row 1:
──────
source_database:               default
source_table:                  source_mt_table
destination_database:          default
destination_table:             destination_table
create_time:                   2025-11-19 09:09:11
part_name:                     20251016-365_1_1_0
destination_file_path:         table_root/eventDate=2025-10-16/retention=365/20251016-365_1_1_0_17B2F6CD5D3C18E787C07AE3DAF16EB1.parquet
elapsed:                       2.04845441
rows_read:                     1138688 -- 1.14 million
total_rows_to_read:            550961374 -- 550.96 million
total_size_bytes_compressed:   37619147120 -- 37.62 billion
total_size_bytes_uncompressed: 138166213721 -- 138.17 billion
bytes_read_uncompressed:       316892925 -- 316.89 million
memory_usage:                  596006095 -- 596.01 million
peak_memory_usage:             601239033 -- 601.24 million
```

