---
description: 'Aggregate function that re-samples time series data to the specified grid.'
sidebar_position: 226
slug: /sql-reference/aggregate-functions/reference/timeSeriesResampleToGridWithStaleness
title: 'timeSeriesResampleToGridWithStaleness'
doc_type: 'reference'
---

Aggregate function that takes time series data as pairs of timestamps and values and re-samples this data to a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the most recent (within the specified time window) sample is chosen.

Alias: `timeSeriesLastToGrid`.

Parameters:
- `start timestamp` - specifies start of the grid
- `end timestamp` - specifies end of the grid
- `grid step` - specifies step of the grid in seconds
- `staleness window` - specified the maximum "staleness" of the most recent sample in seconds

Arguments:
- `timestamp` - timestamp of the sample
- `value` - value of the time series corresponding to the `timestamp`

Return value:
time series values re-sampled to the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there is no sample for a particular grid point.

Example:
The following query re-samples time series data to the grid [90, 105, 120, 135, 150, 165, 180, 195, 210] by choosing the value no older than 30 sec for each point on the grid:

```sql
WITH
    -- NOTE: the gap between 140 and 190 is to show how values are filled for ts = 150, 165, 180 according to staleness window parameter
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 120 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    30 AS window_seconds  -- "staleness" window
SELECT timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
FROM
(
    -- This subquery converts arrays of timestamps and values into rows of `timestamp`, `value`
    SELECT
        arrayJoin(arrayZip(timestamps, values)) AS ts_and_val,
        ts_and_val.1 AS timestamp,
        ts_and_val.2 AS value
);
```

Response:

```response
   ┌─timeSeriesResa⋯stamp, value)─┐
1. │ [NULL,NULL,1,3,4,4,NULL,5,8] │
   └──────────────────────────────┘
```

Also it is possible to pass multiple samples of timestamps and values as Arrays of equal size. The same query with array arguments:

```sql
WITH
    [110, 120, 130, 140, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 1, 3, 4, 5, 5, 8, 12, 13]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 120 AS end_ts,
    15 AS step_seconds,
    30 AS window_seconds
SELECT timeSeriesResampleToGridWithStaleness(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
