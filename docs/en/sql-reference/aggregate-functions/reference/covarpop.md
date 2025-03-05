---
slug: /sql-reference/aggregate-functions/reference/covarpop
sidebar_position: 121
title: "covarPop"
description: "Calculates the population covariance"
---

# covarPop

Calculates the population covariance:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}
$$

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`covarPopStable`](../reference/covarpopstable.md) function. It works slower but provides a lower computational error.
:::

**Syntax**

```sql
covarPop(x, y)
```

**Arguments**

- `x` — first variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).
- `y` — second variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).

**Returned Value**

- The population covariance between `x` and `y`. [Float64](../../data-types/float.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6, -4.4),(2, -9.6, 3),(3, -1.3, -4),(4, 5.3, 9.7),(5, 4.4, 0.037),(6, -8.6, -7.8),(7, 5.1, 9.3),(8, 7.9, -3.6),(9, -8.2, 0.62),(10, -3, 7.3);
```

```sql
SELECT covarPop(x_value, y_value)
FROM series;
```

Result:

```reference
┌─covarPop(x_value, y_value)─┐
│                   6.485648 │
└────────────────────────────┘
```

## Combinators

The following combinators can be applied to the `covarPop` function:

### covarPopIf
Calculates the population covariance only for rows that match the given condition.

### covarPopArray
Calculates the population covariance between elements in two arrays.

### covarPopMap
Calculates the population covariance for each key in the map separately.

### covarPopSimpleState
Returns the covariance value with SimpleAggregateFunction type.

### covarPopState
Returns the intermediate state of covariance calculation.

### covarPopMerge
Combines intermediate covariance states to get the final result.

### covarPopMergeState
Combines intermediate covariance states but returns an intermediate state.

### covarPopForEach
Calculates population covariance for corresponding elements in multiple arrays.

### covarPopDistinct
Calculates the population covariance using distinct value pairs only.

### covarPopOrDefault
Returns 0 if there are not enough rows to calculate covariance.

### covarPopOrNull
Returns NULL if there are not enough rows to calculate covariance.

Note: These combinators can also be applied to the covarPopStable function with the same behavior but using the numerically stable algorithm.
