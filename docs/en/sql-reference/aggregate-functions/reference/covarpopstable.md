---
slug: /en/sql-reference/aggregate-functions/reference/covarpopstable
sidebar_position: 123
---

# covarPopStable

Calculates the value of the population covariance:

$$
\frac{\Sigma{(x - \bar{x})(y - \bar{y})}}{n}
$$

It is similar to the [covarPop](../reference/covarpop.md) function, but uses a numerically stable algorithm. As a result, `covarPopStable` is slower than `covarPop` but produces a more accurate result.


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
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT covarPopStable(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series
);
```

Result:

```reference
┌─covarPopStable(x_value, y_value)─┐
│                         6.485648 │
└──────────────────────────────────┘
```

