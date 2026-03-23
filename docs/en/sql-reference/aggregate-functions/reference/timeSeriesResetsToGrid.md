---
description: 'Aggregate function that calculates PromQL-like resets over time series data on the specified grid.'
sidebar_position: 230
slug: /sql-reference/aggregate-functions/reference/timeSeriesResetsToGrid
title: 'timeSeriesResetsToGrid'
doc_type: 'reference'
---

Aggregate function that takes time series data as pairs of timestamps and values and calculates [PromQL-like resets](https://prometheus.io/docs/prometheus/latest/querying/functions/#resets) from this data on a regular time grid described by start timestamp, end timestamp and step. For each point on the grid the samples for calculating `resets` are considered within the specified time window.

Parameters:
- `start timestamp` - specifies start of the grid
- `end timestamp` - specifies end of the grid
- `grid step` - specifies step of the grid in seconds
- `staleness` - specified the maximum "staleness" in seconds of the considered samples

Arguments:
- `timestamp` - timestamp of the sample
- `value` - value of the time series corresponding to the `timestamp`

Return value:
`resets` values on the specified grid as an `Array(Nullable(Float64))`. The returned array contains one value for each time grid point. The value is NULL if there are no samples within the window to calculate the resets value for a particular grid point.

Example:
The following query calculates `resets` values on the grid [90, 105, 120, 135, 150, 165, 180, 195, 210, 225]:

```sql
WITH
    -- NOTE: the gap between 130 and 190 is to show how values are filled for ts = 180 according to window parameter
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values, -- array of values corresponding to timestamps above
    90 AS start_ts,       -- start of timestamp grid
    90 + 135 AS end_ts,   -- end of timestamp grid
    15 AS step_seconds,   -- step of timestamp grid
    45 AS window_seconds  -- "staleness" window
SELECT timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)
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
   ┌─timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamp, value)─┐
1. │ [NULL,NULL,0,1,1,1,NULL,0,1,2]                                                           │
   └──────────────────────────────────────────────────────────────────────────────────────────┘
```

Also it is possible to pass multiple samples of timestamps and values as Arrays of equal size. The same query with array arguments:

```sql
WITH
    [110, 120, 130, 190, 200, 210, 220, 230]::Array(DateTime) AS timestamps,
    [1, 3, 2, 6, 6, 4, 2, 0]::Array(Float32) AS values,
    90 AS start_ts,
    90 + 135 AS end_ts,
    15 AS step_seconds,
    45 AS window_seconds
SELECT timeSeriesResetsToGrid(start_ts, end_ts, step_seconds, window_seconds)(timestamps, values);
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
