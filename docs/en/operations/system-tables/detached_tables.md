---
slug: /en/operations/system-tables/detached_tables
---
# detached_tables

Contains information of each detached table.

Columns:

- `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the table is in.

- `table` ([String](../../sql-reference/data-types/string.md)) — Table name.

- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — Table uuid (Atomic database).

- `metadata_path` ([String](../../sql-reference/data-types/string.md)) - Path to the table metadata in the file system.

- `is_permanently` ([UInt8](../../sql-reference/data-types/int-uint.md)) - Flag indicates that the table was detached PERMANENTLY.


**Example**

```sql
SELECT * FROM system.detached_tables FORMAT Vertical;
```

```text
Row 1:
──────
database:                   base
table:                      t1
uuid:                       81b1c20a-b7c6-4116-a2ce-7583fb6b6736
metadata_path:              /var/lib/clickhouse/store/461/461cf698-fd0b-406d-8c01-5d8fd5748a91/t1.sql
is_permanently:             1
```
