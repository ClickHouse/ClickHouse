---
title: "varSampStable"
slug: /en/sql-reference/aggregate-functions/reference/varsampstable
sidebar_position: 213
---

## varSampStable

Calculate the sample variance of a data set. Unlike [`varSamp`](../reference/varsamp.md), this function uses a numerically stable algorithm. It works slower but provides a lower computational error.

**Syntax**

```sql
varSampStable(x)
```

Alias: `VAR_SAMP_STABLE`

**Parameters**

- `x`: The population for which you want to calculate the sample variance. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**

- Returns the sample variance of the input data set. [Float64](../../data-types/float.md).

**Implementation details**

The `varSampStable` function calculates the sample variance using the same formula as the [`varSamp`](../reference/varsamp.md):

$$
\sum\frac{(x - \text{mean}(x))^2}{(n - 1)}
$$

Where:
- `x` is each individual data point in the data set.
- `mean(x)` is the arithmetic mean of the data set.
- `n` is the number of data points in the data set.

**Example**

Query:

```sql
DROP TABLE IF EXISTS test_data;
CREATE TABLE test_data
(
    x Float64
)
ENGINE = Memory;

INSERT INTO test_data VALUES (10.5), (12.3), (9.8), (11.2), (10.7);

SELECT round(varSampStable(x),3) AS var_samp_stable FROM test_data;
```

Response:

```response
┌─var_samp_stable─┐
│           0.865 │
└─────────────────┘
```
