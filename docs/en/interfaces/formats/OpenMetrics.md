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

Use `FORMAT OpenMetrics` for input (`INSERT`, table functions such as `file()` and `format()`, and other read paths) and for query output (`SELECT ... FORMAT OpenMetrics`). The registered HTTP content type is `application/openmetrics-text; charset=utf-8`. The output does not advertise OpenMetrics 1.0 compliance because `# HELP`, `# TYPE`, and `# UNIT` are emitted only when the corresponding columns are non-empty and the `type` column value is passed through verbatim (including the Prometheus-style `untyped`).

### Table shape {#table-shape}

Columns follow the same logical model as [Prometheus](./Prometheus.md), extended with `unit`:

- `name` ([String](/sql-reference/data-types/string.md)) and `value` ([Float64](/sql-reference/data-types/float.md)) form the OpenMetrics sample on the wire. Both are typed as above when present in the header; the inferred external schema always includes them.
- Optional: `help`, `type`, and `unit` ([String](/sql-reference/data-types/string.md)), `labels` ([Map(String, String)](/sql-reference/data-types/map.md)), `timestamp` ([Int64](/sql-reference/data-types/int-uint.md) or [Nullable(Int64)](/sql-reference/data-types/nullable.md)).
- The format reports `supports_subsets_of_columns = 1`, so queries that read OpenMetrics text (for example `SELECT name FROM file(..., OpenMetrics)`) may project to any subset of the inferred columns — including subsets that omit `name` or `value`. The parser still walks the `name`/`value` tokens on every sample line for grammar validation; it just skips inserting columns the query did not request.

`type` should be one of `counter`, `gauge`, `histogram`, `summary`, `untyped`, or empty. Histogram and summary label rules follow the [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).

### Timestamp column {#timestamp-column}

The ClickHouse `timestamp` column is **milliseconds since the Unix epoch**, identical to `FORMAT Prometheus`. OpenMetrics text on the wire uses [`realnumber` epoch seconds](https://github.com/prometheus/OpenMetrics/blob/main/specification/OpenMetrics.md#abnf), so the format converts at the boundary:

- **Output:** the `Int64` ms value is emitted as `<seconds>.<3-digit-ms>` with trailing zeros stripped. For example `1520879607789` → `1520879607.789`, `1520879607000` → `1520879607`, `-500` → `-0.5`, `0` → `0`. The boundary values `Int64::min` and `Int64::max` render as `-9223372036854775.808` and `9223372036854775.807`.
- **Input:** the OpenMetrics token (any `realnumber`: integer, fractional, or with exponent) is multiplied by `1000` and stored as `Int64` ms. For integer and fractional tokens (the shapes the writer emits) the conversion uses exact unsigned arithmetic, so values produced by `FORMAT OpenMetrics` round-trip through this format without precision loss — including the boundary tokens above. Fractional digits beyond the third are sub-millisecond and are silently truncated. Tokens whose ms value falls outside `Int64` are rejected with `INCORRECT_DATA`.

### Compared to `FORMAT Prometheus` {#compared-to-prometheus}

| Topic | `Prometheus` | `OpenMetrics` |
|-------|----------------|---------------|
| Input | Not supported | Supported |
| `# UNIT` lines | Not emitted | Emitted when `unit` is non-empty (after `# TYPE`, before samples) |
| End of stream | N/A | Output ends with `# EOF`; input rejects any line with non-whitespace after `# EOF` |
| `timestamp` column | Omitted in the text when the numeric value is `0` | Emitted whenever `timestamp` is not `NULL` (OpenMetrics timestamp rules) |
| `timestamp` unit on the wire | Milliseconds (verbatim) | Epoch seconds with sub-second precision (ms column divided by `1000` on output, multiplied by `1000` on input) |

### Input validation {#input-validation}

Malformed exposition raises `INCORRECT_DATA`, including duplicate label keys, invalid or empty metric/label names, whitespace between a metric name and its `{labels}` block, a missing ASCII space or tab between the metric descriptor and sample value, float tokens that are not fully consumed, invalid timestamp or exemplar tokens (OpenMetrics `realnumber` grammar), timestamps whose ms value (`token * 1000`) overflows `Int64`, trailing characters after the value or timestamp on a sample line, invalid exemplar syntax after `#` (optional exemplar timestamp allowed), a `# EOF` line with extra non-whitespace on the same line, any non-blank content after a valid `# EOF` line, and histogram `_bucket` samples folded into a `# TYPE ... histogram` family that omit the required `le` label.

On **output**, label values are quoted with OpenMetrics-specific escaping (`\\`, `\"`, and `\n` only). Other control characters in a label value raise `BAD_ARGUMENTS`, and duplicate keys in the `labels` map raise `BAD_ARGUMENTS` instead of silently keeping the first entry. Metric names and label keys are validated with the same identifier rules as the input parser before any sample bytes are written; invalid names raise `BAD_ARGUMENTS` rather than emitting text that `FORMAT OpenMetrics` cannot read back.

## Example usage {#example-usage}

See [Prometheus](./Prometheus.md) for a detailed tabular example. The same shape of data can be exported with `FORMAT OpenMetrics` to obtain `# UNIT` lines (when `unit` is set), samples, and a trailing `# EOF` line.

### Exporting from a TimeSeries table {#exporting-from-a-timeseries-table}

`FORMAT OpenMetrics` builds its output from the `name` and `value` columns, plus the optional `help`/`type`/`unit`/`labels`/`timestamp` columns, matched by column name. To export a [TimeSeries](/engines/table-engines/special/time_series) table as OpenMetrics, define a reusable [parameterized view](/sql-reference/statements/create/view#parameterized-view) once that reshapes the engine's data into those columns, then select from it with `FORMAT OpenMetrics`.

TimeSeries data is stored across three target tables, so the view joins the [`timeSeriesSamples`](/sql-reference/table-functions/timeSeriesSamples), [`timeSeriesTags`](/sql-reference/table-functions/timeSeriesTags), and [`timeSeriesMetrics`](/sql-reference/table-functions/timeSeriesMetrics) table functions:

```sql
CREATE TABLE http_metrics ENGINE = TimeSeries;

INSERT INTO http_metrics (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'POST', 'code': '200'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 1027)],
     'http_requests_total', 'counter', 'requests', 'Total number of HTTP requests');

CREATE VIEW openmetrics_exposition AS
SELECT
    CAST(tags.metric_name AS String)                  AS name,
    samples.value                                     AS value,
    coalesce(CAST(metrics.help AS String), '')        AS help,
    coalesce(CAST(metrics.type AS String), '')        AS type,
    coalesce(CAST(metrics.unit AS String), '')        AS unit,
    CAST(mapFilter((k, v) -> (k != '__name__'), tags.tags) AS Map(String, String)) AS labels,
    toUnixTimestamp64Milli(samples.timestamp)         AS timestamp
FROM timeSeriesSamples(http_metrics) AS samples
INNER JOIN timeSeriesTags(http_metrics) AS tags ON samples.id = tags.id
LEFT JOIN timeSeriesMetrics(http_metrics) AS metrics ON tags.metric_name = metrics.metric_family_name
WHERE tags.metric_name = {metric:String};
```

The `timestamp` column must be milliseconds since the Unix epoch (see [Timestamp column](#timestamp-column)), so the view converts the samples' `DateTime64(3)` with `toUnixTimestamp64Milli`. The `__name__` label is dropped because the metric name is exposed separately as `name`.

Export a metric family by passing the parameter at call time:

```sql
SELECT * FROM openmetrics_exposition(metric = 'http_requests_total')
ORDER BY name, timestamp
FORMAT OpenMetrics;
```

```text
# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
# UNIT http_requests_total requests
http_requests_total{code="200",method="POST"} 1027 1704067200

# EOF
```

## Format settings {#format-settings}
