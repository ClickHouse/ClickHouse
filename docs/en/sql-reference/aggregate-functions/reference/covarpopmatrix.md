---
slug: /en/sql-reference/aggregate-functions/reference/covarpopmatrix
sidebar_position: 36
---

# covarPopMatrix

Returns the population covariance matrix over N variables.

**Syntax**

```sql
covarPopMatrix(x[, ...])
```

**Arguments**

- `x` — a variable number of parameters. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal](../../data-types/decimal.md).

**Returned Value**

- Population covariance matrix. [Array](../../data-types/array.md)([Array](../../data-types/array.md)([Float64](../../data-types/float.md))).

**Example**

Query:

```sql
DROP TABLE IF EXISTS series;
CREATE TABLE series(i UInt32, x_value Float64, y_value Float64) ENGINE = Memory;
INSERT INTO series(i, x_value, y_value) VALUES (1, 5.6,-4.4),(2, -9.6,3),(3, -1.3,-4),(4, 5.3,9.7),(5, 4.4,0.037),(6, -8.6,-7.8),(7, 5.1,9.3),(8, 7.9,-3.6),(9, -8.2,0.62),(10, -3,7.3);
```

```sql
SELECT arrayMap(x -> round(x, 3), arrayJoin(covarPopMatrix(a, b, c, d))) AS covarPopMatrix
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
   ┌─covarPopMatrix────────────┐
1. │ [8.25,-1.76,4.08,6.748]   │
2. │ [-1.76,41.07,6.486,2.132] │
3. │ [4.08,6.486,34.21,4.755]  │
4. │ [6.748,2.132,4.755,9.93]  │
   └───────────────────────────┘
```
