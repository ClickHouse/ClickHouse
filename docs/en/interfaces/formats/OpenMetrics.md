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

Malformed exposition raises `INCORRECT_DATA`, including duplicate label keys, invalid or empty metric/label names, whitespace between a metric name and its `{labels}` block, a missing ASCII space or tab between the metric descriptor and sample value, float tokens that are not fully consumed, invalid timestamp or exemplar tokens (OpenMetrics `realnumber` grammar), timestamps whose ms value (`token * 1000`) overflows `Int64`, trailing characters after the value or timestamp on a sample line, invalid exemplar syntax after `#` (optional exemplar timestamp allowed), a `# EOF` line with extra non-whitespace on the same line, and any non-blank content after a valid `# EOF` line.

On **output**, label values are quoted with OpenMetrics-specific escaping (`\\`, `\"`, and `\n` only). Other control characters in a label value raise `BAD_ARGUMENTS`, and duplicate keys in the `labels` map raise `BAD_ARGUMENTS` instead of silently keeping the first entry.

## Example usage {#example-usage}

See [Prometheus](./Prometheus.md) for a detailed tabular example. The same shape of data can be exported with `FORMAT OpenMetrics` to obtain `# UNIT` lines (when `unit` is set), samples, and a trailing `# EOF` line.

## Format settings {#format-settings}
