---
slug: /en/sql-reference/statements/undrop
sidebar_label: UNDROP
---

# UNDROP TABLE

Cancels the dropping of the table.

Beginning with ClickHouse version 23.3 it is possible to UNDROP a table in an Atomic database
within `database_atomic_delay_before_drop_table_sec` (8 minutes by default) of issuing the DROP TABLE statement.  Dropped tables are listed in 
a system table called `system.dropped_tables`.

If you have a materialized view without a `TO` clause associated with the dropped table, then you will also have to UNDROP the inner table of that view.

:::tip
Also see [DROP TABLE](/docs/en/sql-reference/statements/drop.md)
:::

Syntax:

``` sql
UNDROP TABLE [db.]name [UUID '<uuid>'] [ON CLUSTER cluster]
```

**Example**

```sql
CREATE TABLE tab
(
    `id` UInt8
)
ENGINE = MergeTree
ORDER BY id;

DROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;
```

```response
Row 1:
──────
index:                 0
database:              default
table:                 tab
uuid:                  aa696a1a-1d70-4e60-a841-4c80827706cc
engine:                MergeTree
metadata_dropped_path: /var/lib/clickhouse/metadata_dropped/default.tab.aa696a1a-1d70-4e60-a841-4c80827706cc.sql
table_dropped_time:    2023-04-05 14:12:12

1 row in set. Elapsed: 0.001 sec. 
```

```sql
UNDROP TABLE tab;

SELECT *
FROM system.dropped_tables
FORMAT Vertical;

```response
Ok.

0 rows in set. Elapsed: 0.001 sec. 
```

```sql
DESCRIBE TABLE tab
FORMAT Vertical;
```

```response
Row 1:
──────
name:               id
type:               UInt8
default_type:       
default_expression: 
comment:            
codec_expression:   
ttl_expression:     
```
