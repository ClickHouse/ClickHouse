---
slug: /en/sql-reference/aggregate-functions/reference/arrayconcatagg
sidebar_position: 110
---

# array_concat_agg 
- Alias of `groupArrayArray`. The function is case insensitive.

**Exampla**

```text
SELECT *
FROM t

┌─a───────┐
│ [1,2,3] │
│ [4,5]   │
│ [6]     │
└─────────┘

```

Query:

```sql
SELECT array_concat_agg(a) AS a
FROM t

┌─a─────────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```
