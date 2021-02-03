---
toc_title: DISTINCT
---

# DISTINCT Clause {#select-distinct}

If `SELECT DISTINCT` is specified, only unique rows will remain in a query result. Thus only a single row will remain out of all the sets of fully matching rows in the result.

## Null Processing {#null-processing}

`DISTINCT` works with [NULL](../../../sql-reference/syntax.md#null-literal) as if `NULL` were a specific value, and `NULL==NULL`. In other words, in the `DISTINCT` results, different combinations with `NULL` occur only once. It differs from `NULL` processing in most other contexts.

## Alternatives {#alternatives}

It is possible to obtain the same result by applying [GROUP BY](../../../sql-reference/statements/select/group-by.md) across the same set of values as specified as `SELECT` clause, without using any aggregate functions. But there are few differences from `GROUP BY` approach:

-   `DISTINCT` can be applied together with `GROUP BY`.
-   When [ORDER BY](../../../sql-reference/statements/select/order-by.md) is omitted and [LIMIT](../../../sql-reference/statements/select/limit.md) is defined, the query stops running immediately after the required number of different rows has been read.
-   Data blocks are output as they are processed, without waiting for the entire query to finish running.

## Examples {#examples}

ClickHouse supports using the `DISTINCT` and `ORDER BY` clauses for different columns in one query. The `DISTINCT` clause is executed before the `ORDER BY` clause.

Example table:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

When selecting data with the `SELECT DISTINCT a FROM t1 ORDER BY b ASC` query, we get the following result:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

If we change the sorting direction `SELECT DISTINCT a FROM t1 ORDER BY b DESC`, we get the following result:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Row `2, 4` was cut before sorting.

Take this implementation specificity into account when programming queries.
