---
description: 'Reads time series from a TimeSeries table filtered by a selector and with timestamps in a specified interval.'
sidebar_label: 'timeSeriesSelector'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelector
title: 'timeSeriesSelector'
doc_type: 'reference'
---

# timeSeriesSelector Table Function

Reads time series from a TimeSeries table filtered by a selector and with timestamps in a specified interval.
This function is similar to [range selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors) but it's used to implement [instant selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) too.

## Syntax {#syntax}

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `instant_query` - An instant selector written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), without `@` or `offset` modifiers.
- `min_time - Start timestamp, inclusive.
- `max_time - End timestamp, inclusive.

## Returned value {#returned_value}

The function returns three columns:
- `id` - Contains the identifiers of time series matching the specified selector.
- `timestamp` - Contains timestamps.
- `value` - Contains values.

There is no specific order for returned data.

## Example {#example}

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```
