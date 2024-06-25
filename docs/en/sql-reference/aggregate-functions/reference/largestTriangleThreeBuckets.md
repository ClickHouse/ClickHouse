---
slug: /en/sql-reference/aggregate-functions/reference/largestTriangleThreeBuckets
sidebar_position: 159
sidebar_label: largestTriangleThreeBuckets
---

# largestTriangleThreeBuckets

Applies the [Largest-Triangle-Three-Buckets](https://skemman.is/bitstream/1946/15343/3/SS_MSthesis.pdf) algorithm to the input data.
The algorithm is used for downsampling time series data for visualization. It is designed to operate on series sorted by x coordinate.
It works by dividing the sorted series into buckets and then finding the largest triangle in each bucket. The number of buckets is equal to the number of points in the resulting series.
the function will sort data by `x` and then apply the downsampling algorithm to the sorted data.

**Syntax**

``` sql
largestTriangleThreeBuckets(n)(x, y)
```

Alias: `lttb`.

**Arguments**

- `x` — x coordinate. [Integer](../../../sql-reference/data-types/int-uint.md) , [Float](../../../sql-reference/data-types/float.md) , [Decimal](../../../sql-reference/data-types/decimal.md)  , [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md), [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md).
- `y` — y coordinate. [Integer](../../../sql-reference/data-types/int-uint.md) , [Float](../../../sql-reference/data-types/float.md) , [Decimal](../../../sql-reference/data-types/decimal.md)  , [Date](../../../sql-reference/data-types/date.md), [Date32](../../../sql-reference/data-types/date32.md), [DateTime](../../../sql-reference/data-types/datetime.md), [DateTime64](../../../sql-reference/data-types/datetime64.md).

NaNs are ignored in the provided series, meaning that any NaN values will be excluded from the analysis. This ensures that the function operates only on valid numerical data.

**Parameters**

- `n` — number of points in the resulting series. [UInt64](../../../sql-reference/data-types/int-uint.md).

**Returned values**

[Array](../../../sql-reference/data-types/array.md) of [Tuple](../../../sql-reference/data-types/tuple.md) with two elements:

**Example**

Input table:

``` text
┌─────x───────┬───────y──────┐
│ 1.000000000 │ 10.000000000 │
│ 2.000000000 │ 20.000000000 │
│ 3.000000000 │ 15.000000000 │
│ 8.000000000 │ 60.000000000 │
│ 9.000000000 │ 55.000000000 │
│ 10.00000000 │ 70.000000000 │
│ 4.000000000 │ 30.000000000 │
│ 5.000000000 │ 40.000000000 │
│ 6.000000000 │ 35.000000000 │
│ 7.000000000 │ 50.000000000 │
└─────────────┴──────────────┘
```

Query:

``` sql
SELECT largestTriangleThreeBuckets(4)(x, y) FROM largestTriangleThreeBuckets_test;
```

Result:

``` text
┌────────largestTriangleThreeBuckets(4)(x, y)───────────┐
│           [(1,10),(3,15),(9,55),(10,70)]              │
└───────────────────────────────────────────────────────┘
```

