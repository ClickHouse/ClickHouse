---
description: 'Selects the first encountered value of a column.'
sidebar_position: 102
slug: /sql-reference/aggregate-functions/reference/any
title: 'any'
---

# any

Selects the first encountered value of a column.

:::warning
As a query can be executed in arbitrary order, the result of this function is non-deterministic.
If you need an arbitrary but deterministic result, use functions [`min`](../reference/min.md) or [`max`](../reference/max.md).
:::

By default, the function never returns NULL, i.e. ignores NULL values in the input column.
However, if the function is used with the `RESPECT NULLS` modifier, it returns the first value reads no matter if NULL or not.

**Syntax**

```sql
any(column) [RESPECT NULLS]
```

Aliases `any(column)` (without `RESPECT NULLS`)
- `any_value`
- [`first_value`](../reference/first_value.md).

Alias for `any(column) RESPECT NULLS`
- `anyRespectNulls`, `any_respect_nulls`
- `firstValueRespectNulls`, `first_value_respect_nulls`
- `anyValueRespectNulls`, `any_value_respect_nulls`

**Parameters**
- `column`: The column name.

**Returned value**

The first value encountered.

:::note
The return type of the function is the same as the input, except for LowCardinality which is discarded.
This means that given no rows as input it will return the default value of that type (0 for integers, or Null for a Nullable() column).
You might use the `-OrNull` [combinator](../../../sql-reference/aggregate-functions/combinators.md) ) to modify this behaviour.
:::

**Implementation details**

In some cases, you can rely on the order of execution.
This applies to cases when `SELECT` comes from a subquery that uses `ORDER BY`.

When a `SELECT` query has the `GROUP BY` clause or at least one aggregate function, ClickHouse (in contrast to MySQL) requires that all expressions in the `SELECT`, `HAVING`, and `ORDER BY` clauses be calculated from keys or from aggregate functions.
In other words, each column selected from the table must be used either in keys or inside aggregate functions.
To get behavior like in MySQL, you can put the other columns in the `any` aggregate function.

**Example**

Query:

```sql
CREATE TABLE tab (city Nullable(String)) ENGINE=Memory;

INSERT INTO tab (city) VALUES (NULL), ('Amsterdam'), ('New York'), ('Tokyo'), ('Valencia'), (NULL);

SELECT any(city), anyRespectNulls(city) FROM tab;
```

```response
┌─any(city)─┬─anyRespectNulls(city)─┐
│ Amsterdam │ ᴺᵁᴸᴸ                  │
└───────────┴───────────────────────┘
```