---
description: 'Evaluates a prometheus query using data from a TimeSeries table.'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

# prometheusQuery Table Function

Evaluates a prometheus query using data from a TimeSeries table over a range of evaluation times.

## Syntax {#syntax}

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `promql_query` - A query written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/).
- `start_time` - The start time of the evaluation range.
- `end_time` - The end time of the evaluation range.
- `step` - The step used to iterate the evaluation time from `start_time` to `end_time` (inclusively).

## Returned value {#returned_value}

The function can returns different columns depending on the result type of the query passed to parameter `promql_query`:

| Result Type | Result Columns | Example |
|-------------|----------------|---------|
| vector      | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType | prometheusQuery(mytable, 'up') |
| matrix      | tags Array(Tuple(String, String)), time_series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, 'up[1m]') |
| scalar      | scalar ValueType | prometheusQuery(mytable, '1h30m') |
| string      | string String | prometheusQuery(mytable, '"abc"') |

## Example {#example}

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```
