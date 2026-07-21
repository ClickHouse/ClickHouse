---
description: 'Computes the sample skewness of a sequence.'
sidebar_position: 186
slug: /sql-reference/aggregate-functions/reference/skewsamp
title: 'skewSamp'
---

# skewSamp

Computes the [sample skewness](https://en.wikipedia.org/wiki/Skewness) of a sequence.

It represents an unbiased estimate of the skewness of a random variable if passed values form its sample.

```sql
skewSamp(expr)
```

**Arguments**

`expr` — [Expression](/sql-reference/syntax#expressions) returning a number.

**Returned value**

The skewness of the given distribution. Type — [Float64](../../../sql-reference/data-types/float.md). If `n <= 1` (`n` is the size of the sample), then the function returns `nan`.

**Example**

```sql
SELECT skewSamp(value) FROM series_with_value_column;
```
