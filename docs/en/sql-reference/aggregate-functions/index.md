---
slug: /en/sql-reference/aggregate-functions/
sidebar_label: Aggregate Functions
sidebar_position: 33
---

# Aggregate Functions

Aggregate functions work in the [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) way as expected by database experts.

ClickHouse also supports:

- [Parametric aggregate functions](../../sql-reference/aggregate-functions/parametric-functions.md#aggregate_functions_parametric), which accept other parameters in addition to columns.
- [Combinators](../../sql-reference/aggregate-functions/combinators.md#aggregate_functions_combinators), which change the behavior of aggregate functions.


## NULL Processing

During aggregation, all `NULL`s are skipped. If the aggregation has several parameters it will ignore any row in which one or more of the parameters are NULL.

**Examples:**

Consider this table:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Let’s say you need to total the values in the `y` column:

``` sql
SELECT sum(y) FROM t_null_big
```

```text
┌─sum(y)─┐
│      7 │
└────────┘
```

Now you can use the `groupArray` function to create an array from the `y` column:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` does not include `NULL` in the resulting array.

You can use [COALESCE](../../sql-reference/functions/functions-for-nulls.md#coalesce) to change NULL into a value that makes sense in your use case. For example: `avg(COALESCE(column, 0))` with use the column value in the aggregation or zero if NULL:

``` sql
SELECT
    avg(y),
    avg(coalesce(y, 0))
FROM t_null_big
```

``` text
┌─────────────avg(y)─┬─avg(coalesce(y, 0))─┐
│ 2.3333333333333335 │                 1.4 │
└────────────────────┴─────────────────────┘
```

Also you can use [Tuple](/docs/en/sql-reference/data-types/tuple.md) to work around NULL skipping behavior. The a `Tuple` that contains only a `NULL` value is not `NULL`, so the aggregate functions won't skip that row because of that `NULL` value.

```sql
SELECT
    groupArray(y),
    groupArray(tuple(y)).1
FROM t_null_big;

┌─groupArray(y)─┬─tupleElement(groupArray(tuple(y)), 1)─┐
│ [2,2,3]       │ [2,NULL,2,3,NULL]                     │
└───────────────┴───────────────────────────────────────┘
```
