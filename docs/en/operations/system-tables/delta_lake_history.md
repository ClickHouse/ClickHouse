---
description: 'System Delta Lake version history'
keywords: ['system delta_lake_history']
slug: /operations/system-tables/delta_lake_history
title: 'system.delta_lake_history'
doc_type: 'reference'
---

# system.delta_lake_history

This system table contains the version history of Delta Lake tables existing in ClickHouse. It will be empty if you don't have any Delta Lake tables in ClickHouse.

This table is useful for:
- Inspecting the commit history of Delta Lake tables
- Understanding what operations were performed on each version
- Working with the Change Data Feed (CDF) feature to query specific versions

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `version` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Version number of the Delta Lake table.
- `timestamp` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime64](../../sql-reference/data-types/datetime64.md))) — Timestamp when this version was committed.
- `operation` ([String](../../sql-reference/data-types/string.md)) — Operation type (e.g., WRITE, DELETE, MERGE, UPDATE).
- `operation_parameters` ([Map(String, String)](../../sql-reference/data-types/map.md)) — Parameters of the operation.
- `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md)) — Flag indicating if this is the current (latest) version.

**Example**

```sql
SELECT * FROM system.delta_lake_history;
```

```response
┌─database─┬─table──────┬─version─┬───────────────timestamp─┬─operation─┬─operation_parameters─────────────────┬─is_current─┐
│ default  │ my_delta   │       0 │ 2024-01-15 10:30:00.000 │ WRITE     │ {'mode':'Overwrite'}                 │          0 │
│ default  │ my_delta   │       1 │ 2024-01-15 11:45:00.000 │ WRITE     │ {'mode':'Append'}                    │          0 │
│ default  │ my_delta   │       2 │ 2024-01-15 14:20:00.000 │ DELETE    │ {'predicate':'["(id > 100)"]'}       │          1 │
└──────────┴────────────┴─────────┴─────────────────────────┴───────────┴──────────────────────────────────────┴────────────┘
```

**See Also**

- [Delta Lake Table Engine](/docs/en/engines/table-engines/integrations/deltalake.md)
- [system.iceberg_history](iceberg_history.md) — Similar table for Iceberg tables
