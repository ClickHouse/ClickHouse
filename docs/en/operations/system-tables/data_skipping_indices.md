---
description: 'System table containing information about existing data skipping indices
  in all the tables.'
keywords: ['system table', 'data_skipping_indices']
slug: /operations/system-tables/data_skipping_indices
title: 'system.data_skipping_indices'
---

Contains information about existing data skipping indices in all the tables.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `name` ([String](../../sql-reference/data-types/string.md)) — Index name.
- `type` ([String](../../sql-reference/data-types/string.md)) — Index type.
- `type_full` ([String](../../sql-reference/data-types/string.md)) — Index type expression from create statement.
- `expr` ([String](../../sql-reference/data-types/string.md)) — Expression for the index calculation.
- `granularity` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The number of granules in the block.
- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of compressed data, in bytes.
- `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of decompressed data, in bytes.
- `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of marks, in bytes.

**Example**

```sql
SELECT * FROM system.data_skipping_indices LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       user_actions
name:        clicks_idx
type:        minmax
type_full:   minmax
expr:        clicks
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48

Row 2:
──────
database:    default
table:       users
name:        contacts_null_idx
type:        minmax
type_full:   minmax
expr:        assumeNotNull(contacts_null)
granularity: 1
data_compressed_bytes:   58
data_uncompressed_bytes: 6
marks_bytes:             48
```
