---
alias: []
description: 'Documentation for the OpenMetrics text format'
input_format: true
output_format: true
keywords: ['OpenMetrics']
slug: /interfaces/formats/OpenMetrics
title: 'OpenMetrics'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

Reads and writes [OpenMetrics text](https://openmetrics.io/) (Prometheus-compatible exposition text with OpenMetrics extensions).

Use `FORMAT OpenMetrics` for input (`INSERT`, table functions such as `file()` and `format()`, and other read paths) and for query output (`SELECT ... FORMAT OpenMetrics`). The registered HTTP content type is `application/openmetrics-text; version=1.0.0; charset=utf-8`.

### Table shape {#table-shape}

Columns follow the same logical model as [Prometheus](./Prometheus.md), extended with `unit`:

- `name` ([String](/sql-reference/data-types/string.md)) and `value` ([Float64](/sql-reference/data-types/float.md)) are required for input (not `Nullable`/`FixedString` where `String` is required; `value` must be non-nullable `Float64`).
- Optional: `help`, `type`, and `unit` ([String](/sql-reference/data-types/string.md)), `labels` ([Map(String, String)](/sql-reference/data-types/map.md)), `timestamp` ([Nullable(Int64)](/sql-reference/data-types/nullable.md) on input schema; a numeric type on output).

`type` should be one of `counter`, `gauge`, `histogram`, `summary`, `untyped`, or empty. Histogram and summary label rules follow the [Prometheus exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/).

### Compared to `FORMAT Prometheus` {#compared-to-prometheus}

| Topic | `Prometheus` | `OpenMetrics` |
|-------|----------------|---------------|
| Input | Not supported | Supported |
| `# UNIT` lines | Not emitted | Emitted when `unit` is non-empty (after `# TYPE`, before samples) |
| End of stream | N/A | Output ends with `# EOF`; input rejects any line with non-whitespace after `# EOF` |
| `timestamp` column | Omitted in the text when the numeric value is `0` | Emitted whenever `timestamp` is not `NULL` (OpenMetrics timestamp rules) |

### Input validation {#input-validation}

Malformed exposition raises `INCORRECT_DATA`, including duplicate label keys, invalid or empty metric/label names, a missing ASCII space or tab between the metric descriptor and sample value, float tokens that are not fully consumed, trailing characters after the value or timestamp on a sample line, invalid exemplar syntax after `#`, a `# EOF` line with extra non-whitespace on the same line, and any non-blank content after a valid `# EOF` line.

## Example usage {#example-usage}

See [Prometheus](./Prometheus.md) for a detailed tabular example. The same shape of data can be exported with `FORMAT OpenMetrics` to obtain `# UNIT` lines (when `unit` is set), samples, and a trailing `# EOF` line.

## Format settings {#format-settings}
