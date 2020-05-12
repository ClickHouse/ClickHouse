# DISTINCT Clause {#select-distinct}

If DISTINCT is specified, only a single row will remain out of all the sets of fully matching rows in the result.
The result will be the same as if GROUP BY were specified across all the fields specified in SELECT without aggregate functions. But there are several differences from GROUP BY:

-   DISTINCT can be applied together with GROUP BY.
-   When ORDER BY is omitted and LIMIT is defined, the query stops running immediately after the required number of different rows has been read.
-   Data blocks are output as they are processed, without waiting for the entire query to finish running.

DISTINCT is not supported if SELECT has at least one array column.

`DISTINCT` works with [NULL](../../syntax.md#null-literal) as if `NULL` were a specific value, and `NULL=NULL`. In other words, in the `DISTINCT` results, different combinations with `NULL` only occur once.

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
