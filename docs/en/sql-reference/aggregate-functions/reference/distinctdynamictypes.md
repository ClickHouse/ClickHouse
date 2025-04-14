---
slug: /en/sql-reference/aggregate-functions/reference/distinctdynamictypes
sidebar_position: 215
---

# distinctDynamicTypes

Calculates the list of distinct data types stored in [Dynamic](../../data-types/dynamic.md) column.

**Syntax**

```sql
distinctDynamicTypes(dynamic)
```

**Arguments**

- `dynamic` — [Dynamic](../../data-types/dynamic.md) column.

**Returned Value**

- The sorted list of data type names [Array(String)](../../data-types/array.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_dynamic;
CREATE TABLE test_dynamic(d Dynamic) ENGINE = Memory;
INSERT INTO test_dynamic VALUES (42), (NULL), ('Hello'), ([1, 2, 3]), ('2020-01-01'), (map(1, 2)), (43), ([4, 5]), (NULL), ('World'), (map(3, 4))
```

```sql
SELECT distinctDynamicTypes(d) FROM test_dynamic;
```

Result:

```reference
┌─distinctDynamicTypes(d)──────────────────────────────────────┐
│ ['Array(Int64)','Date','Int64','Map(UInt8, UInt8)','String'] │
└──────────────────────────────────────────────────────────────┘
```
