---
slug: /en/sql-reference/aggregate-functions/reference/quantiles
sidebar_position: 201
---

# quantiles Functions

## quantiles

Syntax: `quantiles(level1, level2, …)(x)`

All the quantile functions also have corresponding quantiles functions: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantileInterpolatedWeighted`, `quantilesTDigest`, `quantilesBFloat16`. These functions calculate all the quantiles of the listed levels in one pass, and return an array of the resulting values.

## quantilesExactExclusive

Exactly computes the [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba) Excel function, ([type R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)).

Works more efficiently with sets of levels than [quantileExactExclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactexclusive).

**Syntax**

``` sql
quantilesExactExclusive(level1, level2, ...)(expr)
```

**Arguments**

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

-   `level` — Levels of quantiles. Possible values: (0, 1) — bounds not included. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

-   [Array](../../../sql-reference/data-types/array.md) of quantiles of the specified levels.

Type of array values:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantilesExactExclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.25,499.5,749.75,899.9,949.9499999999999,989.99,998.999]        │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesExactInclusive

Exactly computes the [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) Excel function, ([type R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)).

Works more efficiently with sets of levels than [quantileExactInclusive](../../../sql-reference/aggregate-functions/reference/quantileexact.md#quantileexactinclusive).

**Syntax**

``` sql
quantilesExactInclusive(level1, level2, ...)(expr)
```

**Arguments**

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

-   `level` — Levels of quantiles. Possible values: [0, 1] — bounds included. [Float](../../../sql-reference/data-types/float.md).

**Returned value**

-   [Array](../../../sql-reference/data-types/array.md) of quantiles of the specified levels.

Type of array values:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantilesExactInclusive(0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999)(x)─┐
│ [249.75,499.5,749.25,899.1,949.05,989.01,998.001]                   │
└─────────────────────────────────────────────────────────────────────┘
```

## quantilesApprox

Computes the [quantiles](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence using the [Greenwald-Khanna](http://infolab.stanford.edu/~datar/courses/cs361a/papers/quantiles.pdf) algorithm. The Greenwald-Khanna algorithm is an algorithm used to compute quantiles on a stream of data in a highly efficient manner. It was introduced by Michael Greenwald and Sanjeev Khanna in 2001. It is widely used in databases and big data systems where computing accurate quantiles on a large stream of data in real-time is necessary. The algorithm is highly efficient, taking only O(log n) space and O(log log n) time per item (where n is the size of the input stream). It is also highly accurate, providing approximate quantile values with high probability. 

`quantilesApprox` is different from other quantiles functions in ClickHouse, because it enables user to control the accuracy of the approximate quantiles.

**Syntax**

``` sql
quantilesApprox(accuracy, level1, level2, ...)(expr)
```

**Arguments**

-   `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

-   `accuracy` — Accuracy of quantile. Constant positive integer. Larger accuracy value means less error. For example, if the accuracy argument is set to 100, for example, the computed quantiles will have an error no greater than 1% with high probability. There is a trade-off between the accuracy of the computed quantiles and the computational complexity of the algorithm. A larger accuracy requires more memory and computational resources to compute the quantiles accurately, while a smaller accuracy argument allows for a faster and more memory-efficient computation but with a slightly lower accuracy.

-   `levelN` — Level of quantile. Constant floating-point number from 0 to 1.

**Returned value**

-   [Array](../../../sql-reference/data-types/array.md) of quantiles of the specified levels.

Type of array values:

-   [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
-   [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
-   [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:


``` sql
SELECT quantilesApprox(1, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesApprox(1, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [1,1,1]                                              │
└──────────────────────────────────────────────────────┘

SELECT quantilesApprox(10, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesApprox(10, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [156,413,659]                                         │
└───────────────────────────────────────────────────────┘


SELECT quantilesApprox(100, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesApprox(100, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [251,498,741]                                          │
└────────────────────────────────────────────────────────┘

SELECT quantilesApprox(1000, 0.25, 0.5, 0.75)(number + 1)
FROM numbers(1000)

┌─quantilesApprox(1000, 0.25, 0.5, 0.75)(plus(number, 1))─┐
│ [249,499,749]                                           │
└─────────────────────────────────────────────────────────┘
```
