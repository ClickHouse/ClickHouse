---
description: 'Selects the last encountered value of a column.'
sidebar_position: 105
slug: /sql-reference/aggregate-functions/reference/anylast
title: 'anyLast'
---

# anyLast

Selects the last encountered value of a column.

:::warning
As a query can be executed in arbitrary order, the result of this function is non-deterministic.
If you need an arbitrary but deterministic result, use functions [`min`](../reference/min.md) or [`max`](../reference/max.md).
:::

By default, the function never returns NULL, i.e. ignores NULL values in the input column.
However, if the function is used with the `RESPECT NULLS` modifier, it returns the first value reads no matter if NULL or not.

**Syntax**

```sql
anyLast(column) [RESPECT NULLS]
```

Alias `anyLast(column)` (without `RESPECT NULLS`)
- [`last_value`](../reference/last_value.md).

Aliases for `anyLast(column) RESPECT NULLS`
- `anyLastRespectNulls`, `anyLast_respect_nulls`
- `lastValueRespectNulls`, `last_value_respect_nulls`

**Parameters**
- `column`: The column name.

**Returned value**

- The last value encountered.

**Example**

Query:

```sql
CREATE TABLE tab (city Nullable(String)) ENGINE=Memory;

INSERT INTO tab (city) VALUES ('Amsterdam'),(NULL),('New York'),('Tokyo'),('Valencia'),(NULL);

SELECT anyLast(city), anyLastRespectNulls(city) FROM tab;
```

```response
┌─anyLast(city)─┬─anyLastRespectNulls(city)─┐
│ Valencia      │ ᴺᵁᴸᴸ                      │
└───────────────┴───────────────────────────┘
```
