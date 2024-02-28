---
slug: /en/sql-reference/aggregate-functions/reference/topk
sidebar_position: 108
---

# topK

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves).

Implements the [Filtered Space-Saving](https://doi.org/10.1016/j.ins.2010.08.024) algorithm for analyzing TopK, based on the reduce-and-combine algorithm from [Parallel Space Saving](https://doi.org/10.1016/j.ins.2015.09.003).

``` sql
topK(N)(column)
```

This function does not provide a guaranteed result. In certain situations, errors might occur and it might return frequent values that aren’t the most frequent values.

We recommend using the `N < 10` value; performance is reduced with large `N` values. Maximum value of `N = 65536`.

**Arguments**

- `N` – The number of elements to return.

If the parameter is omitted, default value 10 is used.

**Arguments**

- `x` – The value to calculate frequency.

**Example**

Take the [OnTime](../../../getting-started/example-datasets/ontime.md) data set and select the three most frequently occurring values in the `AirlineID` column.

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```
