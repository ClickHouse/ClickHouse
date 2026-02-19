---
description: 'Calculates the `arg` and `val` value for a maximum `val` value. If there are multiple rows with equal `val` being the maximum, which of the associated `arg` and `val` is returned is not deterministic.'
sidebar_position: 111
slug: /sql-reference/aggregate-functions/reference/argandmax
title: 'argAndMax'
doc_type: 'reference'
---

# argAndMax

Calculates the `arg` and `val` value for a maximum `val` value. If there are multiple rows with equal `val` being the maximum, which of the associated `arg` and `val` is returned is not deterministic.
Both parts the `arg` and the `max` behave as [aggregate functions](/sql-reference/aggregate-functions/index.md), they both [skip `Null`](/sql-reference/aggregate-functions/index.md#null-processing) during processing and return not `Null` values if not `Null` values are available.

:::note 
The only difference with `argMax` is that `argAndMax` returns both argument and value.
:::

**Syntax**

```sql
argAndMax(arg, val)
```

**Arguments**

- `arg` — Argument.
- `val` — Value.

**Returned value**

- `arg` value that corresponds to maximum `val` value.
- `val` maximum `val` value

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
SELECT argAndMax(user, salary) FROM salary;
```

Result:

```text
┌─argAndMax(user, salary)─┐
│ ('director',5000)       │
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
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

SELECT * FROM test;
┌─a────┬────b─┐
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │    3 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

SELECT argMax(a, b), argAndMax(a, b), max(b) FROM test;
┌─argMax(a, b)─┬─argAndMax(a, b)─┬─max(b)─┐
│ b            │ ('b',2)         │      3 │ -- argMax = b because it the first not Null value, max(b) is from another row!
└──────────────┴─────────────────┴────────┘

SELECT argAndMax(tuple(a), b) FROM test;
┌─argAndMax((a), b)─┐
│ ((NULL),3)        │-- The a `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value
└───────────────────┘

SELECT (argMax((a, b), b) as t).1 argMaxA, t.2 argMaxB FROM test;
┌─argMaxA──┬─argMaxB─┐
│ (NULL,3) │       3 │ -- you can use Tuple and get both (all - tuple(*)) columns for the according max(b)
└──────────┴─────────┘

SELECT argAndMax(a, b), max(b) FROM test WHERE a IS NULL AND b IS NULL;
┌─argAndMax(a, b)─┬─max(b)─┐
│ ('',0)          │   ᴺᵁᴸᴸ │-- All aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└─────────────────┴────────┘

SELECT argAndMax(a, (b,a)) FROM test;
┌─argAndMax(a, (b, a))─┐
│ ('c',(2,'c'))        │ -- There are two rows with b=2, `Tuple` in the `Max` allows to get not the first `arg`
└──────────────────────┘

SELECT argAndMax(a, tuple(b)) FROM test;
┌─argAndMax(a, (b))─┐
│ ('b',(2))         │ -- `Tuple` can be used in `Max` to not skip Nulls in `Max`
└───────────────────┘
```

**See also**

- [argMax](/sql-reference/aggregate-functions/reference/argmax.md)
- [Tuple](/sql-reference/data-types/tuple.md)
