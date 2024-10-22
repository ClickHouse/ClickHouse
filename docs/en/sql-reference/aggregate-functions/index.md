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

During aggregation, all `NULL` arguments are skipped. If the aggregation has several arguments it will ignore any row in which one or more of them are NULL.

There is an exception to this rule, which are the functions [`first_value`](../../sql-reference/aggregate-functions/reference/first_value.md), [`last_value`](../../sql-reference/aggregate-functions/reference/last_value.md) and their aliases (`any` and `anyLast` respectively) when followed by the modifier `RESPECT NULLS`. For example, `FIRST_VALUE(b) RESPECT NULLS`.

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

Note that aggregations are skipped when the columns are used as arguments to an aggregated function.  For example [`count`](../../sql-reference/aggregate-functions/reference/count.md) without parameters (`count()`) or with constant ones (`count(1)`) will count all rows in the block (independently of the value of the GROUP BY column as it's not an argument), while `count(column)` will only return the number of rows where column is not NULL.

```sql
SELECT
    v,
    count(1),
    count(v)
FROM
(
    SELECT if(number < 10, NULL, number % 3) AS v
    FROM numbers(15)
)
GROUP BY v

┌────v─┬─count()─┬─count(v)─┐
│ ᴺᵁᴸᴸ │      10 │        0 │
│    0 │       1 │        1 │
│    1 │       2 │        2 │
│    2 │       2 │        2 │
└──────┴─────────┴──────────┘
```

And here is an example of of first_value with `RESPECT NULLS` where we can see that NULL inputs are respected and it will return the first value read, whether it's NULL or not:

```sql
SELECT
    col || '_' || ((col + 1) * 5 - 1) as range,
    first_value(odd_or_null) as first,
    first_value(odd_or_null) IGNORE NULLS as first_ignore_null,
    first_value(odd_or_null) RESPECT NULLS as first_respect_nulls
FROM
(
    SELECT
        intDiv(number, 5) AS col,
        if(number % 2 == 0, NULL, number) as odd_or_null
    FROM numbers(15)
)
GROUP BY col
ORDER BY col

┌─range─┬─first─┬─first_ignore_null─┬─first_respect_nulls─┐
│ 0_4   │     1 │                 1 │                ᴺᵁᴸᴸ │
│ 1_9   │     5 │                 5 │                   5 │
│ 2_14  │    11 │                11 │                ᴺᵁᴸᴸ │
└───────┴───────┴───────────────────┴─────────────────────┘
```
