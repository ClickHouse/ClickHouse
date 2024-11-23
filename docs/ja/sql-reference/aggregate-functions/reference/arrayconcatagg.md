---
slug: /ja/sql-reference/aggregate-functions/reference/array_concat_agg
sidebar_position: 111
---

# array_concat_agg 
- `groupArrayArray`のエイリアスです。この関数は大文字小文字を区別しません。

**例**

```text
SELECT *
FROM t

┌─a───────┐
│ [1,2,3] │
│ [4,5]   │
│ [6]     │
└─────────┘

```

クエリ:

```sql
SELECT array_concat_agg(a) AS a
FROM t

┌─a─────────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```
