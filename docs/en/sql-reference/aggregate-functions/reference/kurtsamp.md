---
description: 'Computes the sample kurtosis of a sequence.'
sidebar_position: 158
slug: /sql-reference/aggregate-functions/reference/kurtsamp
title: 'kurtSamp'
---

# kurtSamp

Computes the [sample kurtosis](https://en.wikipedia.org/wiki/Kurtosis) of a sequence.

It represents an unbiased estimate of the kurtosis of a random variable if passed values form its sample.

```sql
kurtSamp(expr)
```

**Arguments**

`expr` — [Expression](/sql-reference/syntax#expressions) returning a number.

**Returned value**

The kurtosis of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md). If `n <= 1` (`n` is a size of the sample), then the function returns `nan`.

**Example**

```sql
SELECT kurtSamp(value) FROM series_with_value_column;
```
