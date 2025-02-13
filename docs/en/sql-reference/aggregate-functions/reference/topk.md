---
slug: /en/sql-reference/aggregate-functions/reference/topk
sidebar_position: 202
---

# topK

Returns an array of the approximately most frequent values in the specified column. The resulting array is sorted in descending order of approximate frequency of values (not by the values themselves).

Implements the [Filtered Space-Saving](https://doi.org/10.1016/j.ins.2010.08.024) algorithm for analyzing TopK, based on the reduce-and-combine algorithm from [Parallel Space Saving](https://doi.org/10.1016/j.ins.2015.09.003).

``` sql
topK(N)(column)
topK(N, load_factor)(column)
topK(N, load_factor, 'counts')(column)
```

This function does not provide a guaranteed result. In certain situations, errors might occur and it might return frequent values that aren’t the most frequent values.

We recommend using the `N < 10` value; performance is reduced with large `N` values. Maximum value of `N = 65536`.

**Parameters**

- `N` — The number of elements to return. Optional. Default value: 10.
- `load_factor` — Defines, how many cells reserved for values. If uniq(column) > N * load_factor, result of topK function will be approximate. Optional. Default value: 3.
- `counts` — Defines, should result contain approximate count and error value.
 
**Arguments**

- `column` — The value to calculate frequency.

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

**See Also**

- [topKWeighted](../../../sql-reference/aggregate-functions/reference/topkweighted.md)
- [approx_top_k](../../../sql-reference/aggregate-functions/reference/approxtopk.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)