---
slug: /en/sql-reference/aggregate-functions/reference/covarsampmatrix
sidebar_position: 38
---

# covarSampMatrix

Returns the sample covariance matrix over N variables.

**Syntax**

```sql
covarSampMatrix(x[, ...])
```

**Arguments**

- `x` — a variable number of parameters. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).

**Returned Value**

- Sample covariance matrix. [Array](../../data-types/array.md)([Array](../../data-types/array.md)([Float64](../../data-types/float.md))).

**Example**

Query:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT arrayMap(x -> round(x, 3), arrayJoin(covarSampMatrix(a, b, c, d))) AS covarSampMatrix
FROM
(
    SELECT
        a,
        b,
        c,
        d
    FROM test
);
```

Result:

```reference
   ┌─covarSampMatrix─────────────┐
1. │ [9.167,-1.956,4.534,7.498]  │
2. │ [-1.956,45.634,7.206,2.369] │
3. │ [4.534,7.206,38.011,5.283]  │
4. │ [7.498,2.369,5.283,11.034]  │
   └─────────────────────────────┘
```


