---
title: "varPop"
slug: "/en/sql-reference/aggregate-functions/reference/varpop"
sidebar_position: 32
---

This page covers the `varPop` and `varPopStable` functions available in ClickHouse.

## varPop

Calculates the population covariance between two data columns. The population covariance measures the degree to which two variables vary together. Calculates the amount `Σ((x - x̅)^2) / n`, where `n` is the sample size and `x̅`is the average value of `x`.

**Syntax**

```sql
covarPop(x, y)
```

**Parameters**

- `x`: The first data column. [Numeric](../../../native-protocol/columns.md)
- `y`: The second data column. [Numeric](../../../native-protocol/columns.md)

**Returned value**

Returns an integer of type `Float64`.

**Implementation details**

This function uses a numerically unstable algorithm. If you need numerical stability in calculations, use the slower but more stable [`varPopStable` function](#varPopStable).

**Example**

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Int32,
    y Int32
)
ENGINE = Memory;

INSERT INTO test_data VALUES (1, 2), (2, 3), (3, 5), (4, 6), (5, 8);

SELECT
    covarPop(x, y) AS covar_pop
FROM test_data;
```

```response
3
```

## varPopStable

Calculates population covariance between two data columns using a stable, numerically accurate method to calculate the variance. This function is designed to provide reliable results even with large datasets or values that might cause numerical instability in other implementations.

**Syntax**

```sql
covarPopStable(x, y)
```

**Parameters**

- `x`: The first data column. [String literal](../../syntax#syntax-string-literal)
- `y`: The second data column. [Expression](../../syntax#syntax-expressions)

**Returned value**

Returns an integer of type `Float64`.

**Implementation details**

Unlike [`varPop()`](#varPop), this function uses a stable, numerically accurate algorithm to calculate the population variance to avoid issues like catastrophic cancellation or loss of precision. This function also handles `NaN` and `Inf` values correctly, excluding them from calculations.

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Int32,
    y Int32
)
ENGINE = Memory;

INSERT INTO test_data VALUES (1, 2), (2, 9), (9, 5), (4, 6), (5, 8);

SELECT
    covarPopStable(x, y) AS covar_pop_stable
FROM test_data;
```

```response
0.5999999999999999
```
