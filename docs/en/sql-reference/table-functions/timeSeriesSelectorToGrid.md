---
description: 'Reads time series from a TimeSeries table filtered by a selector and with timestamps in a specified interval.'
sidebar_label: 'timeSeriesSelectorToGrid'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelectorToGrid
title: 'timeSeriesSelectorToGrid'
doc_type: 'reference'
---

# timeSeriesSelectorToGrid Table Function

Reads time series from a TimeSeries table filtered by a selector and with timestamps in any of specified intervals:
```
[start_time - window, start_time],
[start_time + step - window, start_time + step],
[start_time + 2 * step - window, start_time + 2 * step],
[start_time + 3 * step - window, start_time + 3 * step],
...
[start_time + N * step - window, start_time + N * step],
```
where N is such number that `start_time + N * step <= end_time` and `start_time + (N + 1) * step > end_time`.

This function is an extended version of [timeSeriesSelector()](./timeSeriesSelector.md) because
```sql
timeSeriesSelector('table_name', 'instant_selector', min_time, max_time)
```
returns the same as
```sql
timeSeriesSelectorToGrid('table_name', 'instant_selector', max_time, max_time, 0, max_time - min_time)
```

## Syntax {#syntax}

```sql
timeSeriesSelectorToGrid('db_name', 'time_series_table', 'instant_selector', start_time, end_time, step, window)
timeSeriesSelectorToGrid(db_name.time_series_table, 'instant_selector', start_time, end_time, step, window)
timeSeriesSelectorToGrid('time_series_table', 'instant_selector', start_time, end_time, step, window)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `instant_selector` - An instant selector written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), without `@` or `offset` modifiers.
- `start time` - Specifies start of the grid.
- `end time` - Specifies end of the grid.
- `step` - Specifies step of the grid in seconds.
- `window` - Specifies the maximum "staleness" in seconds of the considered samples. The staleness window is an interval which includes both of its boundaries.

## Returned value {#returned_value}

The function returns three columns:
- `id` - Contains the identifiers of time series matching the specified selector.
- `timestamp` - Contains timestamps.
- `value` - Contains values.

The function returns data in unspecified order.

## Example {#example}

```sql
SELECT * FROM timeSeriesSelectorToGrid(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE, INTERVAL 30 SECONDS)
```
