---
slug: /en/sql-reference/aggregate-functions/reference/argmin
sidebar_position: 105
---

# argMin

Calculates the `arg` value for a minimum `val` value. If there are several different values of `arg` for minimum values of `val`, returns the first of these values encountered.
Both parts the `arg` and the `min` behave as [aggregate functions](../index.md), they both [skip `Null`](../index.md#null-processing) during processing and return not-Null values if not-Null values are available.

**Syntax**

``` sql
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

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

Query:

``` sql
SELECT argMin(user, salary) FROM salary
```

Result:

``` text
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
FROM values((NULL, 0), ('a', 1), ('b', 2), ('c', 2), (NULL, NULL), ('d', NULL));

select * from test;
┌─a────┬────b─┐
│ ᴺᵁᴸᴸ │    0 │
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

select argMin(a, b), min(b) from test;
┌─argMin(a, b)─┬─min(b)─┐
│ a            │      0 │ -- argMin = a because it the first not Null value, min(b) is from another row!
└──────────────┴────────┘

select argMin(tuple(a), b) from test;
┌─argMin(tuple(a), b)─┐
│ (NULL)              │ -- Tuple allows to get Null value.
└─────────────────────┘

select (argMin((a, b), b) as t).1 argMinA, t.2 argMinB from test;
┌─argMinA─┬─argMinB─┐
│ ᴺᵁᴸᴸ    │       0 │ -- you can use Tuple and get both (all - tuple(*)) columns for the according max(b)
└─────────┴─────────┘

select argMin(a, b), min(b) from test where a is Null and b is Null;
┌─argMin(a, b)─┬─min(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- ll aggregated rows contains at least one `NULL` value because of the filter, so all rows are skipped, therefore the result will be `NULL`
└──────────────┴────────┘

select argMin(a, (b, a)), min(tuple(b, a)) from test;
┌─argMin(a, tuple(b, a))─┬─min(tuple(b, a))─┐
│ d                      │ (NULL,NULL)      │ -- 'd' is the first not Null value for the min
└────────────────────────┴──────────────────┘

select argMin((a, b), (b, a)), min(tuple(b, a)) from test;
┌─argMin(tuple(a, b), tuple(b, a))─┬─min(tuple(b, a))─┐
│ (NULL,NULL)                      │ (NULL,NULL)      │
└──────────────────────────────────┴──────────────────┘

select argMin(a, tuple(b)) from test;
┌─argMax(a, tuple(b))─┐
│ d                   │ -- Tuple can be used in `min` to not skip rows with Null values as b. 
└─────────────────────┘
```

**See also**

- [Tuple](../../data-types/tuple.md)
