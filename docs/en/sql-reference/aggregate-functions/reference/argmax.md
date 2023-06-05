---
slug: /en/sql-reference/aggregate-functions/reference/argmax
sidebar_position: 106
---

# argMax

Calculates the `arg` value for a maximum `val` value. If there are several different values of `arg` for maximum values of `val`, returns the first of these values encountered. 
Both parts the `arg` and the `max` behave as aggregate functions, they skip `Null` during processing and return not-Null values if not-Null values are available.

**Syntax**

``` sql
argMax(arg, val)
```

**Arguments**

- `arg` — Argument.
- `val` — Value.

**Returned value**

- `arg` value that corresponds to maximum `val` value.

Type: matches `arg` type.

**Example**

Input table:

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

Query:

``` sql
SELECT argMax(user, salary) FROM salary;
```

Result:

``` text
┌─argMax(user, salary)─┐
│ director             │
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
FROM values(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

select * from test;
┌─a────┬────b─┐
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │    3 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

select argMax(a, b), max(b) from test;
┌─argMax(a, b)─┬─max(b)─┐
│ b            │      3 │ -- argMax = b because it the first not-Null value, max(b) is from another row!
└──────────────┴────────┘

select argMax(tuple(a), b) from test;
┌─argMax(tuple(a), b)─┐
│ (NULL)              │ -- Tuple allows to get Null value.
└─────────────────────┘

select (argMax((a, b), b) as t).1 argMaxA, t.2 argMaxB from test;
┌─argMaxA─┬─argMaxB─┐
│ ᴺᵁᴸᴸ    │       3 │ -- you can use Tuple and get both (all - tuple(*) ) columns for the according max(b)
└─────────┴─────────┘

select argMax(a, b), max(b) from test where a is Null and b is Null;
┌─argMax(a, b)─┬─max(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- Nulls are not skipped because only Null values are available
└──────────────┴────────┘

select argMax(a, (b,a)) from test;
┌─argMax(a, tuple(b, a))─┐
│ c                      │ -- There are two rows with b=2, Tuple in the `Max` allows to get not the first `arg`
└────────────────────────┘

select argMax(a, tuple(b)) from test;
┌─argMax(a, tuple(b))─┐
│ b                   │ -- Tuple can be used `Max` to not skip Nulls in `Max`
└─────────────────────┘
```

**See also**

- [Tuple](../../data-types/tuple.md)
