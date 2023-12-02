---
slug: /en/sql-reference/aggregate-functions/reference/any
sidebar_position: 6
---

# any

Selects the first encountered value of a column.

By default, it ignores NULL values and returns the first NOT NULL value found in the column. As [`first_value`](../../../sql-reference/aggregate-functions/reference/first_value.md) if supports `RESPECT NULLS`, in which case it will select the first value passed, independently on whether it's NULL or not.

The return type of the function is the same as the input, except for LowCardinality which is discarded). This means that given no rows as input it will return the default value of that type (0 for integers, or Null for a Nullable() column). You might use the `-OrNull` [combinator](../../../sql-reference/aggregate-functions/combinators.md) ) to modify this behaviour.

The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate.
To get a determinate result, you can use the ‘min’ or ‘max’ function instead of ‘any’.

In some cases, you can rely on the order of execution. This applies to cases when SELECT comes from a subquery that uses ORDER BY.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions. To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

- Alias: `any_value`, `first_value`.
