---
slug: /en/operations/system-tables/projections
---
# projections

Contains information about existing projections in all the tables.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
- `name` ([String](../../sql-reference/data-types/string.md)) — Projection name.
- `type` ([Enum](../../sql-reference/data-types/enum.md)) — Projection type ('Normal' = 0, 'Aggregate' = 1).
- `total_rows`, ([UInt64](../../sql-reference/data-types/int-uint.md)) — Total number of rows.
- `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of compressed data, in bytes.
- `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — The size of decompressed data, in bytes.
- `sorting_key` ([Array(String)](../../sql-reference/data-types/array.md)) — Projection sorting key.
- `query` ([String](../../sql-reference/data-types/string.md)) — Projection query.

**Example**

```sql
SELECT * FROM system.projections LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                default
table:                   landing
name:                    improved_sorting_key
type:                    Normal
total_rows:              1000
data_compressed_bytes:   8081
data_uncompressed_bytes: 12890
sorting_key:             ['user_id','date']
query:                   SELECT * ORDER BY user_id, date

Row 2:
──────
database:                default
table:                   landing
name:                    agg
type:                    Aggregate
total_rows:              2
data_compressed_bytes:   82
data_uncompressed_bytes: 32
sorting_key:             ['user_id']
query:                   SELECT user_id, max(date) AS max_date GROUP BY user_id
```
