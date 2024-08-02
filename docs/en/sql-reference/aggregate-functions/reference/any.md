---
slug: /en/sql-reference/aggregate-functions/reference/any
sidebar_position: 102
---

# any

Selects the first encountered value of a column, ignoring any `NULL` values.

**Syntax**

```sql
any(column) [RESPECT NULLS]
```

Aliases: `any_value`, [`first_value`](../reference/first_value.md).

**Parameters**
- `column`: The column name. 

**Returned value**

:::note
Supports the `RESPECT NULLS` modifier after the function name. Using this modifier will ensure the function selects the first value passed, regardless of whether it is `NULL` or not.
:::

:::note
The return type of the function is the same as the input, except for LowCardinality which is discarded. This means that given no rows as input it will return the default value of that type (0 for integers, or Null for a Nullable() column). You might use the `-OrNull` [combinator](../../../sql-reference/aggregate-functions/combinators.md) ) to modify this behaviour.
:::

:::warning
The query can be executed in any order and even in a different order each time, so the result of this function is indeterminate.
To get a determinate result, you can use the [`min`](../reference/min.md) or [`max`](../reference/max.md) function instead of `any`.
:::

**Implementation details**

In some cases, you can rely on the order of execution. This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions. To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

**Example**

Query:

```sql
CREATE TABLE any_nulls (city Nullable(String)) ENGINE=Log;

INSERT INTO any_nulls (city) VALUES (NULL), ('Amsterdam'), ('New York'), ('Tokyo'), ('Valencia'), (NULL);

SELECT any(city) FROM any_nulls;
```

```response
┌─any(city)─┐
│ Amsterdam │
└───────────┘
```
