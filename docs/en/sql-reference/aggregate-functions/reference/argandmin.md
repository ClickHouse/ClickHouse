---
description: 'Calculates the `arg` and `val` value for a minimum `val` value. If there are multiple rows with equal `val` being the minimum, which of the associated `arg` and `val` is returned is not deterministic.'
sidebar_position: 111
slug: /sql-reference/aggregate-functions/reference/argandmin
title: 'argAndMin'
doc_type: 'reference'
---

# argAndMin

Calculates the `arg` and `val` value for a minimum `val` value. If there are multiple rows with equal `val` being the minimum, which of the associated `arg` and `val` is returned is not deterministic.
Both parts the `arg` and the `min` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

:::note
The only difference with `argMin` is that `argAndMin` returns both argument and value.
:::

**Syntax**

```sql
argAndMin(arg, val)
```

**Arguments**

- `arg` — Argument.
- `val` — Value.

**Returned value**

- `arg` value that corresponds to minimum `val` value.
- `val` minimum `val` value

Type: tuple that matches `arg`, `val` types respectively.

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
SELECT argAndMin(user, salary) FROM salary
```

Result:

```text
┌─argAndMin(user, salary)─┐
│ ('worker',1000)         │
└─────────────────────────┘
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

SELECT argMin(a,b), argAndMin(a, b), min(b) FROM test;
┌─argMin(a, b)─┬─argAndMin(a, b)─┬─min(b)─┐
│ a            │ ('a',1)         │      0 │ -- argMin = a because it's the first not `NULL` value, min(b) is from another row!
└──────────────┴─────────────────┴────────┘

SELECT argAndMin(tuple(a), b) FROM test;
┌─argAndMin((a), b)─┐
│ ((NULL),0)        │ -- The 'a' `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value
└───────────────────┘

SELECT (argAndMin((a, b), b) as t).1 argMinA, t.2 argMinB from test;
┌─argMinA──┬─argMinB─┐
│ (NULL,0) │       0 │ -- you can use `Tuple` and get both (all - tuple(*)) columns for the according min(b)
└──────────┴─────────┘

SELECT argAndMin(a, b), min(b) FROM test WHERE a IS NULL and b IS NULL;
┌─argAndMin(a, b)─┬─min(b)─┐
│ ('',0)          │   ᴺᵁᴸᴸ │ -- All aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└─────────────────┴────────┘

SELECT argAndMin(a, (b, a)), min(tuple(b, a)) FROM test;
┌─argAndMin(a, (b, a))─┬─min((b, a))─┐
│ ('a',(1,'a'))        │ (0,NULL)    │ -- 'a' is the first not `NULL` value for the min
└──────────────────────┴─────────────┘

SELECT argAndMin((a, b), (b, a)), min(tuple(b, a)) FROM test;
┌─argAndMin((a, b), (b, a))─┬─min((b, a))─┐
│ ((NULL,0),(0,NULL))       │ (0,NULL)    │ -- argAndMin returns ((NULL,0),(0,NULL)) here because `Tuple` allows to don't skip `NULL` and min(tuple(b, a)) in this case is minimal value for this dataset
└───────────────────────────┴─────────────┘

SELECT argAndMin(a, tuple(b)) FROM test;
┌─argAndMin(a, (b))─┐
│ ('a',(1))         │ -- `Tuple` can be used in `min` to not skip rows with `NULL` values as b.
└───────────────────┘
```

**See also**

- [argMin](/sql-reference/aggregate-functions/reference/argmin.md)
- [Tuple](/sql-reference/data-types/tuple.md)
