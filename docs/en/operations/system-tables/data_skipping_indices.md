# system.data_skipping_indices {#system-data-skipping-indices}

Contains information about existing data skipping indices in all the tables.

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — Database name.
-   `table` ([String](../../sql-reference/data-types/string.md)) — Table name.
-   `name` ([String](../../sql-reference/data-types/string.md)) — Index name.
-   `type` ([String](../../sql-reference/data-types/string.md)) — Index type.
-   `expr` ([String](../../sql-reference/data-types/string.md)) — Expression used to calculate the index.
-   `granularity` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of granules in the block.

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
expr:        clicks
granularity: 1

Row 2:
──────
database:    default
table:       users
name:        contacts_null_idx
type:        minmax
expr:        assumeNotNull(contacts_null)
granularity: 1
```
