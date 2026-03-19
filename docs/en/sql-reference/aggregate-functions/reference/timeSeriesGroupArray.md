---
description: 'Sorts time series by timestamp in ascending order.'
sidebar_position: 146
slug: /sql-reference/aggregate-functions/reference/timeSeriesGroupArray
title: 'timeSeriesGroupArray'
doc_type: 'reference'
---

# timeSeriesGroupArray

Sorts time series by timestamp in ascending order.

**Syntax**

```sql
timeSeriesGroupArray(timestamp, value)
```

**Arguments**

- `timestamp` - timestamp of the sample
- `value` - value of the time series corresponding to the `timestamp`

**Returned value**

The function returns an array of tuples (`timestamp`, `value`) sorted by `timestamp` in ascending order.
If there are multiple values for the same `timestamp` then the function chooses the greatest of these values.

**Example**

```sql
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values -- array of values corresponding to timestamps above
SELECT timeSeriesGroupArray(timestamp, value)
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
   ┌─timeSeriesGroupArray(timestamp, value)───────┐
1. │ [(100,5),(110,1),(120,6),(130,8),(140,19)]   │
   └──────────────────────────────────────────────┘
```

Also it is possible to pass multiple samples of timestamps and values as Arrays of equal size. The same query with array arguments:

```sql
WITH
    [110, 120, 130, 140, 140, 100]::Array(UInt32) AS timestamps,
    [1, 6, 8, 17, 19, 5]::Array(Float32) AS values -- array of values corresponding to timestamps above
SELECT timeSeriesGroupArray(timestamps, values);
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
