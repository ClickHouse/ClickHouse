---
description: 'System table containing a single row with a single `dummy` UInt8 column
  containing the value 0. Similar to the `DUAL` table found in other DBMSs.'
keywords: ['system table', 'one']
slug: /operations/system-tables/one
title: 'system.one'
---

# system.one

This table contains a single row with a single `dummy` UInt8 column containing the value 0.

This table is used if a `SELECT` query does not specify the `FROM` clause.

This is similar to the `DUAL` table found in other DBMSs.

**Example**

```sql
SELECT * FROM system.one LIMIT 10;
```

```response
┌─dummy─┐
│     0 │
└───────┘

1 rows in set. Elapsed: 0.001 sec.
```
