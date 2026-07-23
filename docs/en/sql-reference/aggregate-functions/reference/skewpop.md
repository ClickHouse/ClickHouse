---
description: 'Computes the skewness of a sequence.'
sidebar_position: 185
slug: /sql-reference/aggregate-functions/reference/skewpop
title: 'skewPop'
---

# skewPop

Computes the [skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

```sql
skewPop(expr)
```

**Arguments**

`expr` — [Expression](/sql-reference/syntax#expressions) returning a number.

**Returned value**

The skewness of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md)

**Example**

```sql
SELECT skewPop(value) FROM series_with_value_column;
```
