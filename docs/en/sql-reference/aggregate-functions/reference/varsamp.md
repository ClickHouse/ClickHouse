---
title: "varSamp"
slug: /en/sql-reference/aggregate-functions/reference/varsamp
sidebar_position: 33
---

This page contains information on the `varSamp` and `varSampStable` ClickHouse functions.

## varSamp

Calculate the sample variance of a data set.

**Syntax**

```sql
varSamp(expr)
```

**Parameters**

- `expr`: An expression representing the data set for which you want to calculate the sample variance. [Expression](../../syntax#syntax-expressions)

**Returned value**

Returns a Float64 value representing the sample variance of the input data set.

**Implementation details**

The `varSamp()` function calculates the sample variance using the following formula:

```plaintext
∑(x - mean(x))^2 / (n - 1)
```

Where:

- `x` is each individual data point in the data set.
- `mean(x)` is the arithmetic mean of the data set.
- `n` is the number of data points in the data set.

The function assumes that the input data set represents a sample from a larger population. If you want to calculate the variance of the entire population (when you have the complete data set), you should use the [`varPop()` function](./varpop#varpop) instead.

This function uses a numerically unstable algorithm. If you need numerical stability in calculations, use the slower but more stable [`varSampStable`](#varsampstable) function.

**Example**

Query:

```sql
CREATE TABLE example_table
(
    id UInt64,
    value Float64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO example_table VALUES (1, 10.5), (2, 12.3), (3, 9.8), (4, 11.2), (5, 10.7);

SELECT varSamp(value) FROM example_table;
```

Response:

```response
0.8650000000000091
```

## varSampStable

Calculate the sample variance of a data set using a numerically stable algorithm.

**Syntax**

```sql
varSampStable(expr)
```

**Parameters**

- `expr`: An expression representing the data set for which you want to calculate the sample variance. [Expression](../../syntax#syntax-expressions)

**Returned value**

The `varSampStable` function returns a Float64 value representing the sample variance of the input data set.

**Implementation details**

The `varSampStable` function calculates the sample variance using the same formula as the [`varSamp`](#varsamp) function:

```plaintext
∑(x - mean(x))^2 / (n - 1)
```

Where:
- `x` is each individual data point in the data set.
- `mean(x)` is the arithmetic mean of the data set.
- `n` is the number of data points in the data set.

The difference between `varSampStable` and `varSamp` is that `varSampStable` is designed to provide a more deterministic and stable result when dealing with floating-point arithmetic. It uses an algorithm that minimizes the accumulation of rounding errors, which can be particularly important when dealing with large data sets or data with a wide range of values.

Like `varSamp`, the `varSampStable` function assumes that the input data set represents a sample from a larger population. If you want to calculate the variance of the entire population (when you have the complete data set), you should use the [`varPopStable`](./varpop#varpopstable) function instead.

**Example**

Query:

```sql
CREATE TABLE example_table
(
    id UInt64,
    value Float64
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO example_table VALUES (1, 10.5), (2, 12.3), (3, 9.8), (4, 11.2), (5, 10.7);

SELECT varSampStable(value) FROM example_table;
```

Response:

```response
0.865
```

This query calculates the sample variance of the `value` column in the `example_table` using the `varSampStable()` function. The result shows that the sample variance of the values `[10.5, 12.3, 9.8, 11.2, 10.7]` is approximately 0.865, which may differ slightly from the result of `varSamp` due to the more precise handling of floating-point arithmetic.
