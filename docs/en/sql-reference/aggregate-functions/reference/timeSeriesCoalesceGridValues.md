---
description: 'Combines non-NULL values in arrays from different rows into a single array.'
sidebar_position: 146
slug: /sql-reference/aggregate-functions/reference/timeSeriesCoalesceGridValues
title: 'timeSeriesCoalesceGridValues'
doc_type: 'reference'
---

# timeSeriesCoalesceGridValues

The function finds non-NULL values in the source array of `values`, and then combines
such non-NULL values extracted from different rows into a single result array
retaining the positions of these values. For example,

```sql
CREATE TABLE mytable(a Array(Nullable(Float64))) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO mytable VALUES ([100, NULL, NULL]);
INSERT INTO mytable VALUES ([NULL, 200, NULL]);
INSERT INTO mytable VALUES ([NULL, NULL, 300]);
SELECT timeSeriesCoalesceGridValues('throw_if_conflict')(a) AS result FROM mytable;
```

```response
┌─result────────┐
│ [100,200,300] │
└───────────────┘
```

If all the arrays extracted from different rows have NULL at the same position then the function returns an array with also NULL at that position. The parameter `mode` sets what the function should do if two or more of the arrays have values which are not NULL at the same position.

The main purpose of this function is to be applied to the result of `timeSeries*toGrid` functions,
(for example [timeSeriesRateToGrid()](./timeSeriesRateToGrid.md), [timeSeriesLastToGrid()](./timeSeriesResampleToGridWithStaleness.md)) after modifying the `group` column
with functions like [timeSeriesRemoveTag()](../../functions/time-series-functions#timeSeriesRemoveTag) or
[timeSeriesRemoveAllTagsExcept()](../../functions/time-series-functions#timeSeriesRemoveAllTagsExcept).

**Syntax**

```sql
timeSeriesCoalesceGridValues(mode)(values)
```

**Parameters**
- `mode` - sets what the function should do in case two or more of the arrays extracted from different rows have values which are not `NULL` at the same position. If the `mode` equals to `'null_if_conflict'` then the function returns an array with `NULL` at that position, and if the `mode` equals to `'throw_if_conflict'` then the function throws an exception `Vector cannot contain metrics with the same labelset` (because such error message corresponds to the usage of this function in evaluation of prometheus queries).

**Arguments**

- `values` - array of nullable floating-point values

**Returned value**

The function returns a new array of nullable floating-point values. 

**Example**

```sql
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data
SELECT timeSeriesCoalesceGridValues('null_if_conflict')(arrayJoin(data));
```

Response:

```response
┌─timeSeriesCoalesceGridValues('null_if_conflict')(arrayJoin(data))─┐
│ [1,2,3,NULL,NULL]                                                 │
└───────────────────────────────────────────────────────────────────┘
```

The fifth element in both rows is `5`. So with `mode == null_if_conflict` the function returns an array with `NULL`
at the fifth position, and with `mode == throw_if_conflict` the function throws an exception:

```sql
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data
SELECT timeSeriesCoalesceGridValues('throw_if_conflict')(arrayJoin(data));
```

Response:

```response
Code: 36. DB::Exception: Received from localhost:9000. DB::Exception: Vector cannot contain metrics with the same labelset: While executing ConvertingAggregatedToChunksTransform. (BAD_ARGUMENTS)
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
