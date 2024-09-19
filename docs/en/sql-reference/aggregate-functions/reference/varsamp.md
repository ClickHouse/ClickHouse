---
title: "varSamp"
slug: /en/sql-reference/aggregate-functions/reference/varSamp
sidebar_position: 212
---

## varSamp

Calculate the sample variance of a data set.

**Syntax**

```sql
varSamp(x)
```

Alias: `VAR_SAMP`.

**Parameters**

- `x`: The population for which you want to calculate the sample variance. [(U)Int*](../../data-types/int-uint.md), [Float*](../../data-types/float.md), [Decimal*](../../data-types/decimal.md).

**Returned value**


- Returns the sample variance of the input data set `x`. [Float64](../../data-types/float.md).

**Implementation details**

The `varSamp` function calculates the sample variance using the following formula:

$$
\sum\frac{(x - \text{mean}(x))^2}{(n - 1)}
$$

Where:

- `x` is each individual data point in the data set.
- `mean(x)` is the arithmetic mean of the data set.
- `n` is the number of data points in the data set.

The function assumes that the input data set represents a sample from a larger population. If you want to calculate the variance of the entire population (when you have the complete data set), you should use [`varPop`](../reference/varpop.md) instead.

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

SELECT round(varSamp(x),3) AS var_samp FROM test_data;
```

Response:

```response
┌─var_samp─┐
│    0.865 │
└──────────┘
```
