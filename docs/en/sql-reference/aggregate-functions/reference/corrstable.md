---
slug: /en/sql-reference/aggregate-functions/reference/corrstable
sidebar_position: 107
---

# corrStable

Calculates the Pearson correlation coefficient: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`. Similar to the [`corr`](../reference/corr.md) function, but uses a numerically stable algorithm and works slower for large datasets as a result.

**Syntax**

```sql
corrStable(x, y)
```

**Arguments**

- `x` — first variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).
- `y` — second variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).

**Returned Value**

- The Pearson correlation coefficient. [Float64](../../data-types/float.md).

***Example**

Query:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series
(
    `i` UInt32,
    `x_value` Float64,
    `y_value` Float64
)
ENGINE = Memory
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT corrStable(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series
)
```

Result:

```response
┌─corrStable(x_value, y_value)─┐
│          0.17302657554532558 │
└──────────────────────────────┘
```