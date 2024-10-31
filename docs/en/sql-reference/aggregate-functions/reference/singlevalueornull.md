---
slug: /en/sql-reference/aggregate-functions/reference/singlevalueornull
sidebar_position: 184
---

# singleValueOrNull

The aggregate function `singleValueOrNull` is used to implement subquery operators, such as `x = ALL (SELECT ...)`. It checks if there is only one unique non-NULL value in the data.
If there is only one unique value, it returns it. If there are zero or at least two distinct values, it returns NULL.

**Syntax**

``` sql
singleValueOrNull(x)
```

**Parameters**

- `x` — Column of any [data type](../../data-types/index.md) (except [Map](../../data-types/map.md), [Array](../../data-types/array.md) or [Tuple](../../data-types/tuple) which cannot be of type [Nullable](../../data-types/nullable.md)).

**Returned values**

- The unique value, if there is only one unique non-NULL value in `x`.
- `NULL`, if there are zero or at least two distinct values.

**Examples**

Query:

``` sql
CREATE TABLE test (x UInt8 NULL) ENGINE=Log;
INSERT INTO test (x) VALUES (NULL), (NULL), (5), (NULL), (NULL);
SELECT singleValueOrNull(x) FROM test;
```

Result:

```response
┌─singleValueOrNull(x)─┐
│                    5 │
└──────────────────────┘
```

Query:

```sql
INSERT INTO test (x) VALUES (10);
SELECT singleValueOrNull(x) FROM test;
```

Result:

```response
┌─singleValueOrNull(x)─┐
│                 ᴺᵁᴸᴸ │
└──────────────────────┘
```
