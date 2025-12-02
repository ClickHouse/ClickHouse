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
SELECT timeSeriesCoalesceGridValues('throw')(a) AS result FROM mytable;
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
timeSeriesCoalesceGridValues(mode)(values, [, group])
```

**Parameters**
- `mode` - sets what the function should do in case two or more of the arrays extracted from different rows have values which are not `NULL` at the same position. The following values of the `mode` are supported:

| `mode` | Description |
| --- | --- |
| `'any'` | The function returns the first value it finds at every position. |
| `'null'` | The function returns `NULL` if there are two values at the same position (even if they're equal) |
| `'throw'` | The function throws an exception (`Found duplicate series`) with information about these time series in case the argument `group` is provided |

**Arguments**

- `values` - array of nullable floating-point values
- `group` - [optional] specifies a time series group (see the function [timeSeriesTagsToGroup()](../../functions/time-series-functions#timeSeriesTagsToGroup))

**Returned value**

The function returns a new array of nullable floating-point values. 

**Example**

```sql
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data
SELECT timeSeriesCoalesceGridValues('any')(arrayJoin(data));
```

Response:

```response
┌─timeSeriesCoalesceGridValues('any')(arrayJoin(data))─┐
│ [1,2,3,NULL,5]                                       │
└──────────────────────────────────────────────────────┘
```

The fifth element in both rows is `5`, so if we set `mode` to `null` the function will return `NULL` at the fifth position:


```sql
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data
SELECT timeSeriesCoalesceGridValues('null')(arrayJoin(data));
```

Response:

```response
┌─timeSeriesCoalesceGridValues('null')(arrayJoin(data))─┐
│ [1,2,3,NULL,NULL]                                     │
└───────────────────────────────────────────────────────┘
```

And setting `mode` to `throw` will make the function throw an exception:

```sql
WITH [[1., NULL, 3., NULL, 5], [NULL, 2., NULL, NULL, 5]] AS data
SELECT timeSeriesCoalesceGridValues('throw')(arrayJoin(data));
```

Response:

```response
Code: 36. DB::Exception: Found duplicate time series. (BAD_ARGUMENTS)
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
