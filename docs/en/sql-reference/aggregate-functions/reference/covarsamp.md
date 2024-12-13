---
slug: /en/sql-reference/aggregate-functions/reference/covarsamp
sidebar_position: 124
---

# covarSamp

Calculates the value of `Σ((x - x̅)(y - y̅)) / (n - 1)`.

:::note
This function uses a numerically unstable algorithm. If you need [numerical stability](https://en.wikipedia.org/wiki/Numerical_stability) in calculations, use the [`covarSampStable`](../reference/covarsamp.md) function. It works slower but provides a lower computational error.
:::

**Syntax**

```sql
covarSamp(x, y)
```

**Arguments**

- `x` — first variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).
- `y` — second variable. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).

**Returned Value**

- The sample covariance between `x` and `y`. For `n <= 1`, `nan` is returned. [Float64](../../data-types/float.md).

**Example**

Query:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT covarSamp(x_value, y_value)
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
┌─covarSamp(x_value, y_value)─┐
│           7.206275555555556 │
└─────────────────────────────┘
```

Query:

```sql
SELECT covarSamp(x_value, y_value)
FROM
(
    SELECT
        x_value,
        y_value
    FROM series LIMIT 1
);

```

Result:

```reference
┌─covarSamp(x_value, y_value)─┐
│                         nan │
└─────────────────────────────┘
```


