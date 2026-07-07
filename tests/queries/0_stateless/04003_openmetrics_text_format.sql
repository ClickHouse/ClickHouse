-- FORMAT OpenMetrics: TimeSeries-aligned per-series schema
-- (metric_name, metric_family, help, type, unit, tags, time_series) with OpenMetrics 1.0-conformant
-- output. One row is one series; its samples live in the `time_series` array of (timestamp, value)
-- points. Timestamps are DateTime64(3): the writer emits epoch seconds, the reader converts back.

-- The reader's DateTime64(3) carries no timezone, so its textual rendering follows the session
-- timezone; pin it to UTC so the timestamp columns display deterministically regardless of server TZ.
SET session_timezone = 'UTC';

-- ===== Output =====

-- Minimal series: only metric_name + time_series are required; no metadata, single point at epoch.
SELECT 'http_requests_total' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics;

-- Counter family with metadata and labels: the sample name keeps `_total`, the family name does not.
SELECT 'http_requests_total' AS metric_name, 'http_requests' AS metric_family,
       'Total number of HTTP requests' AS help, 'counter' AS type, '' AS unit,
       [('method', 'GET'), ('code', '200')]::Array(Tuple(String, String)) AS tags,
       [(fromUnixTimestamp64Milli(toInt64(1704067200000)), 1027.0)] AS time_series FORMAT OpenMetrics;

-- Several points in one series emit consecutive sample lines.
SELECT 'temperature' AS metric_name, 'gauge' AS type,
       [(fromUnixTimestamp64Milli(toInt64(0)), 21.5), (fromUnixTimestamp64Milli(toInt64(60000)), 21.7)] AS time_series FORMAT OpenMetrics;

-- Two series sharing a family emit `# TYPE` once (rows are grouped by metric_family).
SELECT metric_name, 'requests' AS metric_family, 'counter' AS type, tags, time_series FROM
(
    SELECT 'requests_total' AS metric_name, [('code', '200')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 5.0)] AS time_series
    UNION ALL
    SELECT 'requests_total', [('code', '500')]::Array(Tuple(String, String)), [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)]
) ORDER BY tags FORMAT OpenMetrics;

-- `untyped` is normalized to `unknown` on output.
SELECT 'x' AS metric_name, 'untyped' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics;

-- `tags` may also be a Map(String, String).
SELECT 'gauge_metric' AS metric_name, 'gauge' AS type, map('region', 'us', 'zone', 'a') AS tags,
       [(fromUnixTimestamp64Milli(toInt64(0)), 42.0)] AS time_series FORMAT OpenMetrics;

-- Timestamp on the wire: sub-second (trailing-zero strip), whole second, epoch 0, negative.
SELECT 'ts' AS metric_name, [
    (fromUnixTimestamp64Milli(toInt64(1520879607789)), 1.0),
    (fromUnixTimestamp64Milli(toInt64(1520879607000)), 2.0),
    (fromUnixTimestamp64Milli(toInt64(0)), 3.0),
    (fromUnixTimestamp64Milli(toInt64(-500)), 4.0)
] AS time_series FORMAT OpenMetrics;

-- Special sample values use the canonical OpenMetrics spellings NaN / +Inf / -Inf.
SELECT 'special' AS metric_name, [
    (fromUnixTimestamp64Milli(toInt64(0)), CAST('nan', 'Float64')),
    (fromUnixTimestamp64Milli(toInt64(0)), CAST('inf', 'Float64')),
    (fromUnixTimestamp64Milli(toInt64(0)), CAST('-inf', 'Float64'))
] AS time_series FORMAT OpenMetrics;

-- A zero-row result still terminates the stream with `# EOF`.
SELECT 'm' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series WHERE 0 FORMAT OpenMetrics;

-- ===== Output: required columns, types, and OpenMetrics 1.0 conformance =====

-- metric_name and time_series are required.
SELECT 'm' AS metric_name FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Column type validation is done up front (avoids assert_cast UB in release builds).
SELECT 'm' AS metric_name, 'x' AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series, 42 AS help FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), toFloat32(1))] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Invalid metric type token.
SELECT 'm' AS metric_name, 'bogus' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Counter suffix contract: the family name must not end with `_total`; each sample must.
SELECT 'foo_total' AS metric_name, 'foo_total' AS metric_family, 'counter' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'foo' AS metric_name, 'foo' AS metric_family, 'counter' AS type, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Unit requires the family name to carry the `_<unit>` suffix.
SELECT 'bytes' AS metric_name, 'bytes' AS metric_family, 'gauge' AS type, 'seconds' AS unit, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Conflicting `# HELP` within one family cannot be represented.
SELECT * FROM (
    SELECT 'cm' AS metric_name, 'cm' AS metric_family, 'a' AS help, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series
    UNION ALL
    SELECT 'cm', 'cm', 'b', [(fromUnixTimestamp64Milli(toInt64(0)), 2.0)]
) ORDER BY time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Invalid metric name, invalid label name, duplicate label, and control chars in a label value are rejected.
SELECT 'bad name' AS metric_name, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [('trace:id', 'x')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [('a', '1'), ('a', '2')]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS metric_name, [('k', concat('a', char(9), 'b'))]::Array(Tuple(String, String)) AS tags, [(fromUnixTimestamp64Milli(toInt64(0)), 1.0)] AS time_series FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== Input =====

-- A counter family: `http_requests_total` folds to metric_family `http_requests`, inheriting metadata.
SELECT metric_name, metric_family, help, type, tags, time_series FROM format(OpenMetrics, $$
# HELP http_requests Total number of HTTP requests
# TYPE http_requests counter
http_requests_total{method="POST",code="200"} 1027 1704067200
# EOF
$$) FORMAT Vertical;

-- Samples of one series are grouped into a single `time_series` array; labels are sorted by name.
SELECT metric_name, tags, time_series FROM format(OpenMetrics, $$
# TYPE q gauge
q{a="1"} 10 1
q{a="1"} 11 2
q{a="2"} 20 1
# EOF
$$) ORDER BY tags FORMAT Vertical;

-- `untyped` is normalized to `unknown` on input.
SELECT metric_name, type FROM format(OpenMetrics, concat('# TYPE u untyped', char(10), 'u 1', char(10), '# EOF', char(10))) FORMAT TSV;

-- A sample with no explicit timestamp is stored at epoch (0).
SELECT time_series FROM format(OpenMetrics, concat('m 5', char(10), '# EOF', char(10))) FORMAT TSV;

-- Exact timestamp round-trip: the writer's second-forms convert back to the original milliseconds.
SELECT toUnixTimestamp64Milli(time_series[1].1) AS ms FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 1520879607.789', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT toUnixTimestamp64Milli(time_series[1].1) AS ms FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 +2', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT toUnixTimestamp64Milli(time_series[1].1) AS ms FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 -.5', char(10), '# EOF', char(10))) FORMAT TSV;
-- Sub-millisecond digits beyond the third are truncated toward zero, not rejected.
SELECT toUnixTimestamp64Milli(time_series[1].1) AS ms FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 1.7895', char(10), '# EOF', char(10))) FORMAT TSV;

-- `tags` may be declared as Map(String, String); labels still sort by name.
SELECT tags FROM format(OpenMetrics, 'metric_name String, tags Map(String, String), time_series Array(Tuple(DateTime64(3), Float64))', concat('m{b="2",a="1"} 5', char(10), '# EOF', char(10))) FORMAT TSV;

-- Column subset projection: the parser validates every line but inserts only requested columns.
SELECT metric_name FROM format(OpenMetrics, concat('foo_bucket{le="0.1"} 3', char(10), '# EOF', char(10))) FORMAT TSV;

-- Inferred external schema.
DESCRIBE TABLE format(OpenMetrics, concat('m 1', char(10), '# EOF', char(10))) FORMAT TSV;

-- ===== Input: grammar and validation =====

-- Trailing garbage after the value / timestamp is rejected (no silent truncation).
SELECT * FROM format(OpenMetrics, concat('m 1 abc', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m 1 2 extra', char(10))); -- { serverError INCORRECT_DATA }
-- Malformed value / timestamp tokens (`tryReadFloatText` alone accepts `.` and `1e+`).
SELECT * FROM format(OpenMetrics, concat('m . 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m 1 1e+', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m 1abc', char(10))); -- { serverError INCORRECT_DATA }
-- Whitespace between the metric name and its `{labels}` block is rejected.
SELECT * FROM format(OpenMetrics, concat('metric {job="x"} 1', char(10))); -- { serverError INCORRECT_DATA }
-- Empty metric name, duplicate label keys, and invalid label names are rejected.
SELECT * FROM format(OpenMetrics, concat('{k="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m{a="1",a="2"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m{trace:id="x"} 1', char(10))); -- { serverError INCORRECT_DATA }
-- Non-finite timestamp tokens are rejected even without a timestamp column (`realnumber` stays strict).
SELECT * FROM format(OpenMetrics, concat('m 1 NaN', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m 1 +Inf', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('m 1 -', char(10))); -- { serverError INCORRECT_DATA }
-- A timestamp whose millisecond value overflows Int64 is rejected.
SELECT * FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1 9223372036854776', char(10))); -- { serverError INCORRECT_DATA }
-- `# EOF` handling: payload after EOF, and non-whitespace on the EOF line, are rejected.
SELECT * FROM format(OpenMetrics, concat('# EOF', char(10), 'm 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# EOF trailing', char(10))); -- { serverError INCORRECT_DATA }
-- Duplicate `# HELP` / `# TYPE` for one family is rejected.
SELECT * FROM format(OpenMetrics, concat('# TYPE h counter', char(10), '# TYPE h gauge', char(10), 'h_total 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# HELP h a', char(10), '# HELP h b', char(10), 'h 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- Metadata must precede the family's samples. The check is keyed on the logical family, so a late
-- `# TYPE h` after an `h_bucket` sample (which folds under `h`) is rejected.
SELECT * FROM format(OpenMetrics, concat('h_bucket{le="1"} 2', char(10), '# TYPE h histogram', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- Column type validation (up front).
SELECT * FROM format(OpenMetrics, 'metric_name String, time_series Int64', concat('m 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'metric_name String, time_series Array(Tuple(DateTime64(3), Float32))', concat('m 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'metric_name FixedString(4), time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'metric_name String, tags Map(String, UInt8), time_series Array(Tuple(DateTime64(3), Float64))', concat('m 1', char(10))); -- { serverError BAD_ARGUMENTS }
