---
slug: /en/sql-reference/statements/undrop
sidebar_label: UNDROP
---

# UNDROP TABLE

Cancels the dropping of the table.

Beginning with ClickHouse version 23.3 it is possible to UNDROP a table 
thin 8 minutes of issuing the DROP TABLE statement.  Dropped tables are listed in 
a system table called `system.dropped_tables`.

:::note
UNDROP TABLE is experimental.  To use it add this setting: 
```sql
set allow_experimental_undrop_table_query = 1;
```
:::

:::tip
Also see [DROP TABLE](/docs/en/sql-reference/statements/drop.md)
:::

Syntax:

``` sql
UNDROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

**Example**

``` sql
set allow_experimental_undrop_table_query = 1;
```

```sql
CREATE TABLE undropMe
(
    `id` UInt8
)
ENGINE = MergeTree
ORDER BY id
```

```sql
DROP TABLE undropMe
```
```sql
SELECT *
FROM system.dropped_tables
FORMAT Vertical
```
```response
Row 1:
──────
index:                 0
database:              default
table:                 undropMe
uuid:                  aa696a1a-1d70-4e60-a841-4c80827706cc
engine:                MergeTree
metadata_dropped_path: /var/lib/clickhouse/metadata_dropped/default.undropMe.aa696a1a-1d70-4e60-a841-4c80827706cc.sql
table_dropped_time:    2023-04-05 14:12:12

1 row in set. Elapsed: 0.001 sec. 
```
```sql
UNDROP TABLE undropMe
```
```response
Ok.
```
```sql
SELECT *
FROM system.dropped_tables
FORMAT Vertical
```
```response
Ok.

0 rows in set. Elapsed: 0.001 sec. 
```
```sql
DESCRIBE TABLE undropMe
FORMAT Vertical
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
