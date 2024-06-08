---
slug: /en/operations/system-tables/dropped_tables
---
# dropped_tables

Contains information about tables that drop table has been executed but data cleanup has not been actually performed.

Columns:

- `index` ([UInt32](../../sql-reference/data-types/int-uint.md)) — Index in marked_dropped_tables queue.
- `database` ([String](../../sql-reference/data-types/string.md)) — Database.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Table uuid.
- `engine` ([String](../../sql-reference/data-types/string.md)) — Table engine name.
- `metadata_dropped_path` ([String](../../sql-reference/data-types/string.md)) — Path of table's metadata file in metadata_dropped directory.
- `table_dropped_time` ([DateTime](../../sql-reference/data-types/datetime.md)) — The time when the next attempt to remove table's data is scheduled on. Usually it's the table when the table was dropped plus `database_atomic_delay_before_drop_table_sec`

**Example**

The following example shows how to get information about dropped_tables.

``` sql
SELECT *
FROM system.dropped_tables\G
```

``` text
Row 1:
──────
index:                 0
database:              default
table:                 test
uuid:                  03141bb2-e97a-4d7c-a172-95cc066bb3bd
engine:                MergeTree
metadata_dropped_path: /data/ClickHouse/build/programs/data/metadata_dropped/default.test.03141bb2-e97a-4d7c-a172-95cc066bb3bd.sql
table_dropped_time:    2023-03-16 23:43:31
```
