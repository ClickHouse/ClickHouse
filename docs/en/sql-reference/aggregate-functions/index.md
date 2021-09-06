---
toc_folder_title: Aggregate Functions
toc_priority: 33
toc_title: Introduction
---

# Aggregate Functions {#aggregate-functions}

Aggregate functions work in the [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) way as expected by database experts.

ClickHouse also supports:

-   [Parametric aggregate functions](../../sql-reference/aggregate-functions/parametric-functions.md#aggregate_functions_parametric), which accept other parameters in addition to columns.
-   [Combinators](../../sql-reference/aggregate-functions/combinators.md#aggregate_functions_combinators), which change the behavior of aggregate functions.


## NULL Processing {#null-processing}

During aggregation, all `NULL`s are skipped.

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


[Original article](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
