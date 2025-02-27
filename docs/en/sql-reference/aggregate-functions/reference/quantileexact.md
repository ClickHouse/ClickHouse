---
slug: /en/sql-reference/aggregate-functions/reference/quantileexact
sidebar_position: 173
---

# quantileExact Functions

## quantileExact

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileExact(level)(expr)
```

Alias: `medianExact`.

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Result:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

## quantileExactLow

Similar to `quantileExact`, this computes the exact [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get the exact value, all the passed values are combined into an array, which is then fully sorted.  The sorting [algorithm's](https://en.cppreference.com/w/cpp/algorithm/sort) complexity is `O(N·log(N))`, where `N = std::distance(first, last)` comparisons.

The return value depends on the quantile level and the number of elements in the selection, i.e. if the level is 0.5, then the function returns the lower median value for an even number of elements and the middle median value for an odd number of elements. Median is calculated similarly to the [median_low](https://docs.python.org/3/library/statistics.html#statistics.median_low) implementation which is used in python.

For all other levels, the element at the index corresponding to the value of `level * size_of_array` is returned. For example:

``` sql
SELECT quantileExactLow(0.1)(number) FROM numbers(10)

┌─quantileExactLow(0.1)(number)─┐
│                             1 │
└───────────────────────────────┘
```

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileExactLow(level)(expr)
```

Alias: `medianExactLow`.

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileExactLow(number) FROM numbers(10)
```

Result:

``` text
┌─quantileExactLow(number)─┐
│                        4 │
└──────────────────────────┘
```
## quantileExactHigh

Similar to `quantileExact`, this computes the exact [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

All the passed values are combined into an array, which is then fully sorted, to get the exact value.  The sorting [algorithm's](https://en.cppreference.com/w/cpp/algorithm/sort) complexity is `O(N·log(N))`, where `N = std::distance(first, last)` comparisons.

The return value depends on the quantile level and the number of elements in the selection, i.e. if the level is 0.5, then the function returns the higher median value for an even number of elements and the middle median value for an odd number of elements. Median is calculated similarly to the [median_high](https://docs.python.org/3/library/statistics.html#statistics.median_high) implementation which is used in python. For all other levels, the element at the index corresponding to the value of `level * size_of_array` is returned.

This implementation behaves exactly similar to the current `quantileExact` implementation.

When using multiple `quantile*` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles) function.

**Syntax**

``` sql
quantileExactHigh(level)(expr)
```

Alias: `medianExactHigh`.

**Arguments**

- `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` value in the range of `[0.01, 0.99]`. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median).
- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
SELECT quantileExactHigh(number) FROM numbers(10)
```

Result:

``` text
┌─quantileExactHigh(number)─┐
│                         5 │
└───────────────────────────┘
```

## quantileExactExclusive

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.EXC](https://support.microsoft.com/en-us/office/percentile-exc-function-bbaa7204-e9e1-4010-85bf-c31dc5dce4ba) Excel function, ([type R6](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)).

When using multiple `quantileExactExclusive` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantilesExactExclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactexclusive) function.

**Syntax**

``` sql
quantileExactExclusive(level)(expr)
```

**Arguments**

- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

- `level` — Level of quantile. Optional. Possible values: (0, 1) — bounds not included. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median). [Float](../../../sql-reference/data-types/float.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactExclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantileExactExclusive(0.6)(x)─┐
│                          599.6 │
└────────────────────────────────┘
```

## quantileExactInclusive

Exactly computes the [quantile](https://en.wikipedia.org/wiki/Quantile) of a numeric data sequence.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memory, where `n` is a number of values that were passed. However, for a small number of values, the function is very effective.

This function is equivalent to [PERCENTILE.INC](https://support.microsoft.com/en-us/office/percentile-inc-function-680f9539-45eb-410b-9a5e-c1355e5fe2ed) Excel function, ([type R7](https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample)).

When using multiple `quantileExactInclusive` functions with different levels in a query, the internal states are not combined (that is, the query works less efficiently than it could). In this case, use the [quantilesExactInclusive](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantilesexactinclusive) function.

**Syntax**

``` sql
quantileExactInclusive(level)(expr)
```

**Arguments**

- `expr` — Expression over the column values resulting in numeric [data types](../../../sql-reference/data-types/index.md#data_types), [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md).

**Parameters**

- `level` — Level of quantile. Optional. Possible values: [0, 1] — bounds included. Default value: 0.5. At `level=0.5` the function calculates [median](https://en.wikipedia.org/wiki/Median). [Float](../../../sql-reference/data-types/float.md).

**Returned value**

- Quantile of the specified level.

Type:

- [Float64](../../../sql-reference/data-types/float.md) for numeric data type input.
- [Date](../../../sql-reference/data-types/date.md) if input values have the `Date` type.
- [DateTime](../../../sql-reference/data-types/datetime.md) if input values have the `DateTime` type.

**Example**

Query:

``` sql
CREATE TABLE num AS numbers(1000);

SELECT quantileExactInclusive(0.6)(x) FROM (SELECT number AS x FROM num);
```

Result:

``` text
┌─quantileExactInclusive(0.6)(x)─┐
│                          599.4 │
└────────────────────────────────┘
```

**See Also**

- [median](../../../sql-reference/aggregate-functions/reference/median.md#median)
- [quantiles](../../../sql-reference/aggregate-functions/reference/quantiles.md#quantiles)
