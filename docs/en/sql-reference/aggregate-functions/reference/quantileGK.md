---
slug: /en/sql-reference/aggregate-functions/reference/quantileGK
sidebar_position: 175
---

# quantileGK

Computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [Greenwald-Khanna](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf) algorithm. The Greenwald-Khanna algorithm is an algorithm used to compute quantiles on a stream of data in a highly efficient manner. It was introduced by Michael Greenwald and Sanjeev Khanna in 2001. It is widely used in databases and big data systems where computing accurate quantiles on a large stream of data in real-time is necessary. The algorithm is highly efficient, taking only O(log n) space and O(log log n) time per item (where n is the size of the input). It is also highly accurate, providing an approximate quantile value with high probability.

`quantileGK` is different from other quantile functions in ClickHouse, because it enables user to control the accuracy of the approximate quantile result.

**Syntax**

``` sql
quantileGK(accuracy, level)(expr)
```

Alias: `medianGK`.

**Arguments**

- `accuracy` — Accuracy of quantile. Constant positive integer. Larger accuracy value means less error. For example, if the accuracy argument is set to 100, the computed quantile will have an error no greater than 1% with high probability. There is a trade-off between the accuracy of the computed quantiles and the computational complexity of the algorithm. A larger accuracy requires more memory and computational resources to compute the quantile accurately, while a smaller accuracy argument allows for a faster and more memory-efficient computation but with a slightly lower accuracy.

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).

- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).


**Returned value**

- Quantile of the specified level and accuracy.


Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

``` sql
SELECT quantileGK(1, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(1, 0.25)(plus(number, 1))─┐
│                                    1 │
└──────────────────────────────────────┘

SELECT quantileGK(10, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(10, 0.25)(plus(number, 1))─┐
│                                   156 │
└───────────────────────────────────────┘

SELECT quantileGK(100, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(100, 0.25)(plus(number, 1))─┐
│                                    251 │
└────────────────────────────────────────┘

SELECT quantileGK(1000, 0.25)(number + 1)
FROM numbers(1000)

┌─quantileGK(1000, 0.25)(plus(number, 1))─┐
│                                     249 │
└─────────────────────────────────────────┘
```


**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
