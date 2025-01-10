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
- `sorting_key` ([Array(String)](../../sql-reference/data-types/array.md)) — Projection sorting key.
- `query` ([String](../../sql-reference/data-types/string.md)) — Projection query.

**Example**

```sql
SELECT * FROM system.projections LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:    default
table:       landing
name:        improved_sorting_key
type:        Normal
sorting_key: ['user_id','date']
query:       SELECT * ORDER BY user_id, date

Row 2:
──────
database:    default
table:       landing
name:        agg_no_key
type:        Aggregate
sorting_key: []
query:       SELECT count()
```
