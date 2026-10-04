---
alias: []
description: 'Documentation for the OpenMetrics text format'
input_format: true
output_format: true
keywords: ['OpenMetrics']
sidebar_label: 'OpenMetrics'
sidebar_position: 30
slug: /interfaces/formats/OpenMetrics
title: 'OpenMetrics'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Reads and writes [OpenMetrics text](https://openmetrics.io/) (Prometheus-compatible exposition text with OpenMetrics extensions).

Use `FORMAT OpenMetrics` for input (`INSERT`, table functions such as `file()` and `format()`, and other read paths) and for query output (`SELECT ... FORMAT OpenMetrics`). The registered HTTP content type is `application/openmetrics-text; version=1.0.0; charset=utf-8`, and the writer produces [OpenMetrics 1.0](https://prometheus.io/docs/specs/om/open_metrics_spec/)-conformant exposition (see [OpenMetrics 1.0 conformance](#openmetrics-10-conformance)).

The column model is aligned with the [TimeSeries](/engines/table-engines/special/time_series) table engine: one row is one time series (a `metric_name` and its `tags`), and its samples are collected into a `time_series` array of `(timestamp, value)` points. This is the same shape the engine exposes, so a TimeSeries table can be exported to OpenMetrics without reshaping the sample-level data (see [Exporting from a TimeSeries table](#exporting-from-a-timeseries-table)).

### Table shape {#table-shape}

| Column | Type | Role |
|--------|------|------|
| `metric_name` | [String](/sql-reference/data-types/string.md) | On-wire sample name, including any suffix (`http_requests_total`, `foo_bucket`, `foo_count`). |
| `time_series` | [Array](/sql-reference/data-types/array.md)([Tuple](/sql-reference/data-types/tuple.md)([DateTime64(3)](/sql-reference/data-types/datetime64.md), [Float64](/sql-reference/data-types/float.md))) | The series' `(timestamp, value)` points. |
| `metric_family` | [String](/sql-reference/data-types/string.md) | Family name used in `# HELP`/`# TYPE`/`# UNIT` (`http_requests`, `foo`). Optional; defaults to `metric_name`. |
| `help` | [String](/sql-reference/data-types/string.md) | `# HELP` text. Optional. |
| `type` | [String](/sql-reference/data-types/string.md) | `# TYPE` value. Optional. |
| `unit` | [String](/sql-reference/data-types/string.md) | `# UNIT` value. Optional. |
| `tags` | [Array](/sql-reference/data-types/array.md)([Tuple](/sql-reference/data-types/tuple.md)([String](/sql-reference/data-types/string.md), [String](/sql-reference/data-types/string.md))) or [Map(String, String)](/sql-reference/data-types/map.md) | The series' labels. Optional. |

`metric_name` and `time_series` are required on output; the inferred external schema (used by `file()`/`format()` when no structure is given) contains all seven columns, with `tags` inferred as `Array(Tuple(String, String))`. Both `tags` spellings are accepted on input and output.

The format reports `supports_subsets_of_columns = 1`, so a query that reads OpenMetrics text may project to any subset of the inferred columns (for example `SELECT metric_name FROM file(..., OpenMetrics)`). The parser still validates every sample line's grammar; it just skips inserting columns the query did not request.

### Row grouping {#row-grouping}

- **Input** buffers the whole exposition to `# EOF`, then emits one row per `(metric_name, tags)` series with all of that series' points collected into `time_series`. Labels are sorted by name so a series' `tags` array is deterministic. `metric_family` is derived from the sample name and the declared `# TYPE` metadata (for example a `foo_bucket` sample under `# TYPE foo histogram` gets `metric_family = foo`).
- **Output** groups consecutive rows by `metric_family` (or by `metric_name` when `metric_family` is empty) to emit `# HELP`/`# TYPE`/`# UNIT` once per family, then one sample line per point. Rows must therefore be ordered so that each family is contiguous — add `ORDER BY metric_family` (or `metric_name`) to the query. Conflicting `help`/`type`/`unit` values within one family raise `BAD_ARGUMENTS`.

### Timestamp handling {#timestamp-handling}

Point timestamps are carried as `DateTime64(3)` inside the `time_series` tuple. OpenMetrics text on the wire uses [`realnumber` epoch seconds](https://prometheus.io/docs/specs/om/open_metrics_spec/#abnf), so the format converts at the boundary:

- **Output:** the timestamp is emitted as `<seconds>.<3-digit-ms>` with trailing zeros stripped. For example `2018-03-12 15:53:27.789` → `1520879607.789`, a whole second → `1520879607`, `1970-01-01 00:00:00` → `0`.
- **Input:** the OpenMetrics token (any `realnumber`: integer, fractional, or with exponent) is converted to the target `DateTime64` scale with exact unsigned arithmetic, so values produced by `FORMAT OpenMetrics` round-trip without precision loss. Fractional digits finer than the target scale are truncated toward zero. A sample line without an explicit timestamp is stored at epoch (`0`). Tokens whose value falls outside the `Int64` range of the timestamp are rejected with `INCORRECT_DATA`.

### OpenMetrics 1.0 conformance {#openmetrics-10-conformance}

The writer enforces the [OpenMetrics 1.0](https://prometheus.io/docs/specs/om/open_metrics_spec/) structural rules, raising `BAD_ARGUMENTS` when a row cannot be represented conformantly:

- **Metric type vocabulary.** `type` must be one of `unknown`, `gauge`, `counter`, `stateset`, `info`, `histogram`, `gaugehistogram`, `summary`, or empty. The Prometheus spelling `untyped` is normalized to `unknown` on both input and output.
- **Counter suffix contract.** A `counter` family name must **not** end with `_total`, and each of its sample names must end with `_total` (or `_created`). The `_total` suffix lives on `metric_name`, never on `metric_family`.
- **Unit suffix rule.** When `unit` is set, `metric_family` must end with `_<unit>`.
- **Exposition structure.** `# HELP`/`# TYPE`/`# UNIT` are emitted at most once per family; there are no blank lines between families; the stream ends with `# EOF`.

The reader is lenient (per the robustness principle): it accepts Prometheus-style exposition, normalizes `untyped` → `unknown`, and does not require the strict counter/unit suffix contract on input.

### Compared to `FORMAT Prometheus` {#compared-to-prometheus}

| Topic | `Prometheus` | `OpenMetrics` |
|-------|----------------|---------------|
| Input | Not supported | Supported |
| Row model | One row per sample | One row per series (`time_series` array of points) |
| `# UNIT` lines | Not emitted | Emitted when `unit` is set |
| End of stream | N/A | Output ends with `# EOF`; input rejects non-whitespace after `# EOF` |
| Timestamp on the wire | Milliseconds | Epoch seconds with sub-second precision |
| OpenMetrics 1.0 | No | Yes (writer) |

### Input validation {#input-validation}

Malformed exposition raises `INCORRECT_DATA`, including duplicate label keys, invalid or empty metric/label names, whitespace between a metric name and its `{labels}` block, a missing ASCII space or tab between the metric descriptor and sample value, float tokens that are not fully consumed, invalid timestamp or exemplar tokens (OpenMetrics `realnumber` grammar), timestamps whose value overflows the target `Int64`, trailing characters after the value or timestamp on a sample line, invalid exemplar syntax after `#`, a duplicate `# HELP`/`# TYPE`/`# UNIT` for the same family, a descriptor that follows the family's samples, a `# EOF` line with extra non-whitespace on the same line, and any non-blank content after a valid `# EOF` line.

On **output**, label values are quoted with OpenMetrics-specific escaping (`\\`, `\"`, and `\n` only); other control characters in a label value raise `BAD_ARGUMENTS`, as do duplicate keys within one series' `tags` and invalid metric/label names.

## Example usage {#example-usage}

### Constructing rows directly {#constructing-rows-directly}

```sql
SELECT *
FROM format(OpenMetrics, $$
# HELP http_requests Total number of HTTP requests
# TYPE http_requests counter
http_requests_total{code="200",method="POST"} 1027 1704067200
http_requests_total{code="200",method="GET"} 34 1704067200
# EOF
$$)
FORMAT Vertical;
```

Each `(metric_name, tags)` series becomes one row, with its samples collected into `time_series`.

### Exporting from a TimeSeries table {#exporting-from-a-timeseries-table}

Because the columns mirror the [TimeSeries](/engines/table-engines/special/time_series) engine, exporting is a projection of the engine's own columns. TimeSeries data is stored across three target tables, so a reusable [parameterized view](/sql-reference/statements/create/view#parameterized-view) joins the [`timeSeriesSamples`](/sql-reference/table-functions/timeSeriesSamples), [`timeSeriesTags`](/sql-reference/table-functions/timeSeriesTags), and [`timeSeriesMetrics`](/sql-reference/table-functions/timeSeriesMetrics) table functions and collects each series' points with `groupArray`:

```sql
CREATE TABLE http_metrics ENGINE = TimeSeries;

INSERT INTO http_metrics (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'POST', 'code': '200'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 1027)],
     'http_requests', 'counter', '', 'Total number of HTTP requests');

CREATE VIEW openmetrics_exposition AS
SELECT
    CAST(tags.metric_name AS String)                                       AS metric_name,
    CAST(metrics.metric_family_name AS String)                            AS metric_family,
    coalesce(CAST(metrics.help AS String), '')                            AS help,
    coalesce(CAST(metrics.type AS String), '')                            AS type,
    coalesce(CAST(metrics.unit AS String), '')                            AS unit,
    CAST(mapFilter((k, v) -> (k != '__name__'), tags.tags) AS Map(String, String)) AS tags,
    groupArray((samples.timestamp, samples.value))                        AS time_series
FROM timeSeriesSamples(http_metrics) AS samples
INNER JOIN timeSeriesTags(http_metrics) AS tags ON samples.id = tags.id
LEFT JOIN timeSeriesMetrics(http_metrics) AS metrics ON tags.metric_name = metrics.metric_family_name
WHERE metrics.metric_family_name = {metric:String}
GROUP BY metric_name, metric_family, help, type, unit, tags.tags;
```

The `__name__` label is dropped because the metric name is exposed separately as `metric_name`.

Export a metric family by passing the parameter at call time (order by family so each family's descriptors are emitted once):

```sql
SELECT * FROM openmetrics_exposition(metric = 'http_requests')
ORDER BY metric_family, metric_name
FORMAT OpenMetrics;
```

```text
# HELP http_requests Total number of HTTP requests
# TYPE http_requests counter
http_requests_total{code="200",method="POST"} 1027 1704067200
# EOF
```

:::note Round-tripping a whole table
Once `SELECT * FROM <timeseries_table>` is supported ([#106010](https://github.com/ClickHouse/ClickHouse/pull/106010)), a TimeSeries table will export directly with `SELECT * FROM http_metrics ORDER BY metric_family FORMAT OpenMetrics`, and the exposition will read back with `INSERT INTO http_metrics FORMAT OpenMetrics`.
:::

## Format settings {#format-settings}
