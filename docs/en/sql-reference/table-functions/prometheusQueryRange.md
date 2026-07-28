---
description: 'Evaluates a prometheus query using data from a TimeSeries table.'
sidebar_label: 'prometheusQueryRange'
sidebar_position: 145
slug: /sql-reference/table-functions/prometheusQueryRange
title: 'prometheusQueryRange'
doc_type: 'reference'
---

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

## Supported PromQL Features {#supported-promql-features}

### Selectors {#selectors}

Instant selectors, range selectors, label matchers (`=`, `!=`, `=~`, `!~`), offset modifiers, `@` timestamp modifiers, and subqueries.

### Functions {#functions}

| Category | Functions |
|----------|-----------|
| Range    | `rate`, `irate`, `delta`, `idelta`, `last_over_time` |
| Math     | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg` |
| Trig     | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Type     | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other    | `time`, `pi` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not yet supported, and the `phi` (quantile level) argument must currently be a constant scalar — expressions that vary per step such as `histogram_quantile(time() / 1000, ...)` are rejected with a `NOT_IMPLEMENTED` error.

### Operators {#operators}

All arithmetic (`+`, `-`, `*`, `/`, `%`, `^`), comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`), and logical (`and`, `or`, `unless`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

Not yet supported: `count_values`.

## Example {#example}

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```
