---
description: 'Calculates the `arg` value for a minimum `val` value. If there are multiple
  rows with equal `val` being the maximum, which of the associated `arg` is returned
  is not deterministic.'
sidebar_position: 110
slug: /sql-reference/aggregate-functions/reference/argmin
title: 'argMin'
---

# argMin

Calculates the `arg` value for a minimum `val` value. If there are multiple rows with equal `val` being the maximum, which of the associated `arg` is returned is not deterministic.
Both parts the `arg` and the `min` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

**Syntax**

```sql
argMin(arg, val)
```

**Arguments**

- `arg` — Argument.
- `val` — Value.

**Returned value**

- `arg` value that corresponds to minimum `val` value.

Type: matches `arg` type.

**Example**

Input table:

```text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

Query:

```sql
SELECT argMin(user, salary) FROM salary
```

Result:

```text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

**Extended example**

```sql
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

SELECT * FROM test;
┌─a────┬────b─┐
│ ᴺᵁᴸᴸ │    0 │
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

SELECT argMin(a, b), min(b) FROM test;
┌─argMin(a, b)─┬─min(b)─┐
│ a            │      0 │ -- argMin = a because it the first not `NULL` value, min(b) is from another row!
└──────────────┴────────┘

SELECT argMin(tuple(a), b) FROM test;
┌─argMin(tuple(a), b)─┐
│ (NULL)              │ -- The a `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value
└─────────────────────┘

SELECT (argMin((a, b), b) as t).1 argMinA, t.2 argMinB from test;
┌─argMinA─┬─argMinB─┐
│ ᴺᵁᴸᴸ    │       0 │ -- you can use `Tuple` and get both (all - tuple(*)) columns for the according max(b)
└─────────┴─────────┘

SELECT argMin(a, b), min(b) FROM test WHERE a IS NULL and b IS NULL;
┌─argMin(a, b)─┬─min(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- All aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└──────────────┴────────┘

SELECT argMin(a, (b, a)), min(tuple(b, a)) FROM test;
┌─argMin(a, tuple(b, a))─┬─min(tuple(b, a))─┐
│ d                      │ (NULL,NULL)      │ -- 'd' is the first not `NULL` value for the min
└────────────────────────┴──────────────────┘

SELECT argMin((a, b), (b, a)), min(tuple(b, a)) FROM test;
┌─argMin(tuple(a, b), tuple(b, a))─┬─min(tuple(b, a))─┐
│ (NULL,NULL)                      │ (NULL,NULL)      │ -- argMin returns (NULL,NULL) here because `Tuple` allows to don't skip `NULL` and min(tuple(b, a)) in this case is minimal value for this dataset
└──────────────────────────────────┴──────────────────┘

SELECT argMin(a, tuple(b)) FROM test;
┌─argMin(a, tuple(b))─┐
│ d                   │ -- `Tuple` can be used in `min` to not skip rows with `NULL` values as b.
└─────────────────────┘
```

**See also**

- [Tuple](/sql-reference/data-types/tuple.md)
