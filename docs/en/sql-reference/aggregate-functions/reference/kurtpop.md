---
description: 'Computes the kurtosis of a sequence.'
sidebar_position: 157
slug: /sql-reference/aggregate-functions/reference/kurtpop
title: 'kurtPop'
---

# kurtPop

Computes the [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

```sql
kurtPop(expr)
```

**Arguments**

`expr` — [Expression](/sql-reference/syntax#expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md)

**Example**

```sql
SELECT kurtPop(value) FROM series_with_value_column;
```
