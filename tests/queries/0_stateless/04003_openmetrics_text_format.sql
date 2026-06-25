-- FORMAT OpenMetrics output/input round-trip. The `timestamp` column is Prometheus-compatible
-- milliseconds; the writer divides by 1000 to emit OpenMetrics seconds and the reader multiplies back.

-- Output: minimal counter; 100 ms -> `0.1` seconds; ms-to-seconds boundary cases (trailing-zero strip, INT64_MIN).
SELECT 'http_requests_total' AS name, 1. AS value, '' AS help, '' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 'http_requests_total' AS name, 2. AS value, 'Total number of HTTP requests' AS help, 'counter' AS type, map('method', 'GET', 'status', '200') AS labels, CAST(100 AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 'a' AS name, 1.0 AS value, CAST(1520879607789 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'b' AS name, 2.0 AS value, CAST(1520879607000 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'c' AS name, 3.0 AS value, CAST(-500 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'd' AS name, 4.0 AS value, CAST(0 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
-- INT64_MIN via bit math (avoid the literal parsing as `-(UInt64{2^63})`).
SELECT 'e' AS name, 5.0 AS value, CAST(bitShiftLeft(toInt64(1), 63) AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;

-- Zero-row result still terminates the stream with `# EOF`.
SELECT 'm' AS name, 1.0 AS value WHERE 0 FORMAT OpenMetrics;

-- Output schema: `timestamp` must be Int64 / Nullable(Int64); other numeric types are rejected (clientError under FORMAT).
SELECT name, value, CAST(timestamp AS Float64) AS timestamp FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp) FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT name, value, CAST(timestamp AS UInt32) AS timestamp FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp) FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== Input parsing and validation =====

-- Timestamp tokens are epoch seconds; reader multiplies by 1000 (fractional ms preserved). `999`s -> 999000 ms.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
$$
# HELP demo_jobs_processed_total Number of completed batch jobs
# TYPE demo_jobs_processed_total counter
# UNIT demo_jobs_processed_total seconds
demo_jobs_processed_total 42 999
# EOF
$$) FORMAT TSV;

-- Tab is valid whitespace between metric descriptor and value.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('ingress_http_requests_total', char(9), '7', char(10), '# EOF', char(10))) FORMAT TSV;

-- Escaped newline / quote / backslash inside a label value (\n, \", \\).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('demo_log_lines_total{k="a', char(92), 'n', 'b"} 1', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('demo_error_messages_total{k="a', char(92), '"', 'b"} 1', char(10), '# EOF', char(10))) FORMAT TSV;

-- Reject trailing garbage after the value / timestamp (no silent truncation).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 abc', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 2 extra', char(10))); -- { serverError INCORRECT_DATA }

-- Reject `#` tails that are not a valid exemplar (`# {labels} <value>`).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 # not_an_exemplar', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 # {broken', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 #{trace_id="x"} 0.5', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 # {trace_id="x"}0.5', char(10))); -- { serverError INCORRECT_DATA }

-- Reject malformed number / realnumber tokens (`tryReadFloatText` alone accepts `.` and `1e+`).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m . 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 1e+', char(10))); -- { serverError INCORRECT_DATA }

-- Valid exemplar suffix is accepted (labels not ingested); an optional exemplar timestamp is allowed.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('demo_http_requests_total 1 # {trace_id="abc"} 0.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('foo_bucket{le="10"} 17 # {trace_id="x"} 9.8 1520879607.789', char(10), '# EOF', char(10))) FORMAT TSV;

-- Reject whitespace between metric name and `{labels}`.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('metric {job="x"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- realnumber timestamp tokens (`+2`, fractional `1520879607.789`); stored ms = token_seconds * 1000.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 +2', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 1520879607.789', char(10), '# EOF', char(10))) FORMAT TSV;
-- Decimal token below 1 second (`-.5`) preserves the sign as `-500` ms.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -.5', char(10), '# EOF', char(10))) FORMAT TSV;

-- Reject empty metric name, duplicate label keys, missing descriptor/value separator, and malformed label names.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('{k="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{a="1",a="2"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{job="x"}1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{a ="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{trace:id="x"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject non-finite timestamp tokens even when the schema has no timestamp column.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 NaN', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 +Inf', char(10))); -- { serverError INCORRECT_DATA }
-- `unit` must be String for the output schema.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, unit UInt8', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }
-- Reject a malformed float sample (no partial parse, e.g. `1abc` must not become `1`).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1abc', char(10))); -- { serverError INCORRECT_DATA }
-- Reject payload after `# EOF`, non-whitespace on the `# EOF` line, and a `-`-only timestamp token.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF', char(10), 'm 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF trailing', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 -', char(10))); -- { serverError INCORRECT_DATA }

-- Reject incompatible declared column types (validated up front; avoids release assert_cast UB).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help UInt64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'name String, value Nullable(Float64)', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'name FixedString(16), value Float64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- `_sum` / `_count` fold into the base name only for histogram/summary (counter/gauge keep the suffix).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('# HELP http_requests_total help\n', '# TYPE http_requests_total counter\n', 'http_requests_total_sum 1\n', '# EOF\n')) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('# HELP http_request_duration_seconds help\n', '# TYPE http_request_duration_seconds histogram\n', 'http_request_duration_seconds_sum 5\n', '# EOF\n')) FORMAT TSV;

-- `# TYPE` ignores trailing spaces on the type token; reject non-whitespace after it.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('# HELP rpc_duration_seconds help\n', '# TYPE rpc_duration_seconds histogram   \n', 'rpc_duration_seconds_bucket{le="0.5"} 3\n', '# EOF\n')) FORMAT TSV;
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram garbage', char(10), 'h_bucket{le="1"} 2', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- Range guards (token seconds, stored seconds*1000 ms): INT64_MAX/1000 ok; +1s overflows; huge float tokens rejected.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854775', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854776', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -9223372036854776', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 1e20', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -1e20', char(10))); -- { serverError INCORRECT_DATA }

-- Exact boundary round-trip for the tokens emitted at Int64::min / Int64::max ms (exact integer/decimal math).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854775.807', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -9223372036854775.808', char(10), '# EOF', char(10))) FORMAT TSV;
-- One ms past the boundary on either side throws.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854775.808', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -9223372036854775.809', char(10))); -- { serverError INCORRECT_DATA }
-- Sub-millisecond fractional digits beyond the third are truncated, not rejected.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 1.7895', char(10), '# EOF', char(10))) FORMAT TSV;

-- `markFormatSupportsSubsetOfColumns`: the parser always reads name/value but inserts only the selected columns.
SELECT name FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT value FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT timestamp FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT labels FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10))) FORMAT TSV;

-- Type-specific suffix folding: `_bucket` is histogram-only (under summary it stays its own metric with `le` kept),
-- `_sum`/`_count` fold for both, and a collision with a user non-empty `sum`/`count` label is rejected.
SELECT name, value, labels, type FROM format(OpenMetrics, concat('# TYPE x summary', char(10), 'x_bucket{le="0.5"} 3', char(10), '# EOF', char(10))) ORDER BY name, value FORMAT TSV;
SELECT name, value, labels, type FROM format(OpenMetrics, concat('# TYPE x summary', char(10), 'x_sum 5', char(10), 'x_count 7', char(10), '# EOF', char(10))) ORDER BY name, value FORMAT TSV;
SELECT name, value, labels, type FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_bucket{le="0.5"} 3', char(10), '# EOF', char(10))) ORDER BY name, value FORMAT TSV;
SELECT * FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_bucket 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_bucket{job="a"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT name, value, labels FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_sum{sum=""} 5', char(10), 'x_count{count=""} 7', char(10), '# EOF', char(10))) ORDER BY name, value FORMAT TSV;
SELECT * FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_sum{sum="bytes"} 5', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE x summary', char(10), 'x_count{count="oops"} 7', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- Output: `le -> _bucket` is histogram-only; on a summary `le` stays a normal label (here next to a
-- valid `quantile` sample). `sum`/`count` are markers only when empty; a non-empty value collides with
-- the reserved marker and is rejected (see the reviewer-finding cases below).
SELECT 'h' AS name, 3.0 AS value, '' AS help, 'histogram' AS type, map('le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 's' AS name, 3.0 AS value, '' AS help, 'summary' AS type, map('quantile', '0.9', 'le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 'bytes_total' AS name, 1.0 AS value, '' AS help, 'counter' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, 'bytes' AS unit FORMAT OpenMetrics;
SELECT 'h' AS name, 9.0 AS value, '' AS help, 'histogram' AS type, map('sum', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 's' AS name, 11.0 AS value, '' AS help, 'summary' AS type, map('count', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;

-- A histogram/summary row must represent exactly one sample kind (bucket/quantile, `_sum`, or `_count`).
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('sum', '', 'le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 2.0 AS value, '' AS help, 'histogram' AS type, map('count', '', 'le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_sum{le="0.5"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h{sum="",le="0.5"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('sum', '', 'count', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 2.0 AS value, '' AS help, 'summary' AS type, map('sum', '', 'quantile', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_sum{count=""} 3', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's_sum{quantile="0.5"} 4', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- Output validates every buffered row once the family `type` is known (not only the current row).
SELECT * FROM (SELECT 'h' AS name, 1.0 AS value, '' AS help, '' AS type, map('sum', '', 'count', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit UNION ALL SELECT 'h', 2.0, '', 'histogram', map('le', '0.5'), CAST(NULL AS Nullable(Int64)), '') ORDER BY value FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- Overflowing timestamp tokens are rejected even when `timestamp` is not projected.
SELECT name FROM format(OpenMetrics, concat('m 1 1e20', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- ===== Reviewer findings: marker collision, zero-kind rows, duplicate metadata, output type validation =====
-- (1) A histogram bucket carrying a real non-empty `count`/`sum` label collides with the `_count`/`_sum`
-- marker synthesized for that series, so it is rejected (was: emitted `_count`/`_sum` for the wrong series).
SELECT 'h' AS name, 9.0 AS value, '' AS help, 'histogram' AS type, map('count', 'partition', 'le', '+Inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 9.0 AS value, '' AS help, 'histogram' AS type, map('sum', 'x', 'le', '1') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- (2) A typed histogram/summary row with no sample kind (no bucket/quantile, no `_sum`/`_count` marker) is rejected on output and input.
SELECT 'h' AS name, 3.0 AS value, '' AS help, 'histogram' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 3.0 AS value, '' AS help, 'summary' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h 3', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's 3', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- (3) Duplicate `# HELP` / `# TYPE` / `# UNIT` for the same family is rejected (order-dependent overwrite).
SELECT * FROM format(OpenMetrics, concat('# TYPE h counter', char(10), '# TYPE h gauge', char(10), 'h 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# HELP h a', char(10), '# HELP h b', char(10), 'h 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# UNIT h_seconds seconds', char(10), '# UNIT h_seconds ms', char(10), 'h_seconds 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- (4) Output `type` is emitted raw in `# TYPE`; reject values with whitespace or control characters that would break the stream.
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'counter garbage' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 1.0 AS value, '' AS help, concat('coun', char(10), 'ter') AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Histogram `+Inf` / `_count` synthesis preserves non-marker labels per series.
SELECT 'req' AS name, 10.0 AS value, '' AS help, 'histogram' AS type, map('job', 'api', 'count', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT * FROM (
    SELECT 'req' AS name, 5.0 AS value, '' AS help, 'histogram' AS type, map('job', 'api', 'le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
    UNION ALL
    SELECT 'req', 10.0, '', 'histogram', map('job', 'api', 'count', ''), CAST(NULL AS Nullable(Int64)), ''
    UNION ALL
    SELECT 'req', 20.0, '', 'histogram', map('job', 'web', 'le', '+Inf'), CAST(NULL AS Nullable(Int64)), ''
) ORDER BY value FORMAT OpenMetrics;

-- Output label serialization: only `\\`,`\"`,`\n` escapes; other control chars / duplicate keys / bad names rejected.
SELECT 'm' AS name, 1.0 AS value, map('k', concat('a', char(10), 'b')) AS labels FORMAT OpenMetrics;
SELECT 'm' AS name, 1.0 AS value, map('k', concat('a', char(9), 'b')) AS labels FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS name, 1.0 AS value, map('a', '1', 'a', '2') AS labels FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'bad name' AS name, 1.0 AS value FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'm' AS name, 1.0 AS value, map('trace:id', 'x') AS labels FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- ===== Regression: histogram `+Inf` / `_count` synthesis is keyed per (series, timestamp) =====
-- Same series sampled at two timestamps: a `_count` marker at one and a `+Inf` bucket at the other.
-- Each timestamp must get its own synthesized counterpart; a series-only key would cross-match the
-- two timestamps and suppress both syntheses, leaving an incomplete histogram at each timestamp.
SELECT * FROM (
    SELECT 'lat' AS name, 7.0 AS value, '' AS help, 'histogram' AS type, map('count', '') AS labels, CAST(1000 AS Nullable(Int64)) AS timestamp, '' AS unit
    UNION ALL
    SELECT 'lat', 9.0, '', 'histogram', map('le', '+Inf'), CAST(2000 AS Nullable(Int64)), ''
) ORDER BY timestamp FORMAT OpenMetrics;
-- Two `_count` markers for the same series at different timestamps each get their own synthesized
-- `+Inf` (a series-only key would overwrite the marker row and emit only one `+Inf`).
SELECT * FROM (
    SELECT 'lat2' AS name, 3.0 AS value, '' AS help, 'histogram' AS type, map('count', '') AS labels, CAST(1000 AS Nullable(Int64)) AS timestamp, '' AS unit
    UNION ALL
    SELECT 'lat2', 5.0, '', 'histogram', map('count', ''), CAST(2000 AS Nullable(Int64)), ''
) ORDER BY timestamp FORMAT OpenMetrics;

-- ===== Reviewer findings (round 2): case-insensitive `number`, counter `_total` metadata, boundary-label validation =====

-- M1: OpenMetrics `number` special values are ASCII case-insensitive, and inf may be spelled inf/infinity
-- with an optional sign; `m nan`, `m +infinity`, `m -INFINITY`, `m InF` parse and round-trip to the
-- canonical NaN / +Inf / -Inf on output.
SELECT name, value FROM format(OpenMetrics, 'name String, value Float64', concat('m_nan nan', char(10), 'm_pinf +infinity', char(10), 'm_ninf -INFINITY', char(10), 'm_inf InF', char(10), '# EOF', char(10))) ORDER BY name FORMAT OpenMetrics;
-- M1: the case-insensitive `number` rule must NOT leak into the timestamp position — `realnumber` stays
-- strict and keeps rejecting NaN / Inf in every spelling.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 NaN', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 nan', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 infinity', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 -inf', char(10))); -- { serverError INCORRECT_DATA }

-- M2: a `counter` family `foo` exposes its sample as `foo_total`; the row inherits type/help/unit from
-- the base family but keeps the name `foo_total` (the `_total` suffix is not dropped).
SELECT name, value, help, type, unit FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('# HELP foo total foos', char(10), '# TYPE foo counter', char(10), '# UNIT foo bars', char(10), 'foo_total 5', char(10), '# EOF', char(10))) FORMAT TSV;
-- M2: inheritance is counter-only — a `_total` under a non-counter family stays an ordinary metric with no metadata.
SELECT name, value, type FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('# TYPE foo gauge', char(10), 'foo_total 5', char(10), '# EOF', char(10))) FORMAT TSV;
-- M2: unsupported sibling suffixes (`_created` / `_gcount` / `_gsum`) under a declared family are rejected.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# TYPE foo counter', char(10), 'foo_created 5', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# TYPE foo gauge', char(10), 'foo_gcount 5', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# TYPE foo summary', char(10), 'foo_gsum 5', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
-- M2: do NOT over-reject — a standalone `_created` / `_total` whose base family is undeclared ingests as an ordinary metric.
SELECT name, value, type FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('x_created 7', char(10), '# EOF', char(10))) FORMAT TSV;
SELECT name, value, type FROM format(OpenMetrics, 'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String', concat('lonely_total 9', char(10), '# EOF', char(10))) FORMAT TSV;

-- M3: histogram `le` / summary `quantile` boundary labels are validated with a strict parse BEFORE the
-- numeric sort (the sort's lenient parse would silently map garbage to 0). Invalid boundaries are
-- rejected; the canonical `+Inf` bucket and an in-range quantile still succeed.
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', 'NaN') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', '-Inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', 'garbage') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 1.0 AS value, '' AS help, 'summary' AS type, map('quantile', 'bogus') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 1.0 AS value, '' AS help, 'summary' AS type, map('quantile', '2') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 1.0 AS value, '' AS help, 'summary' AS type, map('quantile', 'NaN') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', '+Inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;
SELECT 's' AS name, 1.0 AS value, '' AS help, 'summary' AS type, map('quantile', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;

-- ===== Reviewer findings (round 3): boundary-label validation is strict and symmetric (input + output) =====
-- One shared validator (`OpenMetricsText::checkBoundaryLabel`) drives both sides. It uses the strict
-- `realnumber` tokenizer, so malformed tokens that raw `tryReadFloatText` accepts (`.`, `1e+`) are
-- rejected, and only the exact token `+Inf` denotes the histogram infinity bucket (every other
-- spelling is rejected, which also prevents a duplicate synthesized `+Inf` bucket).

-- INPUT: invalid histogram `le` / summary `quantile` boundaries are now rejected (was: accepted on
-- input, asymmetric with the output path). INCORRECT_DATA matches the surrounding input error style.
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="NaN"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's{quantile="2"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="."} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="1e+"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- OUTPUT: the malformed tokens that the old lenient `tryReadFloatText` check accepted (`.`, `1e+`)
-- are rejected by the strict tokenizer.
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', '.') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 's' AS name, 1.0 AS value, '' AS help, 'summary' AS type, map('quantile', '1e+') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- Infinity policy: non-canonical infinity spellings (`inf`, `Inf`) are rejected on BOTH input and
-- output. Previously the output accepted them case-insensitively but `fixupBucketLabels` only
-- recognized the exact `+Inf`, so a DUPLICATE synthetic `+Inf` bucket was generated.
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="inf"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="Inf"} 1', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', 'inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT 'h' AS name, 1.0 AS value, '' AS help, 'histogram' AS type, map('le', 'Inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }

-- A valid `+Inf` bucket round-trips to exactly ONE `+Inf` bucket: the exact-token check recognizes it
-- as the existing infinity bucket, so the writer synthesizes only the `_count` counterpart (no second `+Inf`).
SELECT 'h' AS name, 4.0 AS value, '' AS help, 'histogram' AS type, map('le', '+Inf') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit FORMAT OpenMetrics;

-- INPUT positives: a finite `le` (including the canonical `+Inf` bucket) and an in-range `quantile` are accepted.
SELECT name, value, labels, type FROM format(OpenMetrics, concat('# TYPE h histogram', char(10), 'h_bucket{le="0.5"} 3', char(10), 'h_bucket{le="+Inf"} 9', char(10), '# EOF', char(10))) ORDER BY value FORMAT TSV;
SELECT name, value, labels, type FROM format(OpenMetrics, concat('# TYPE s summary', char(10), 's{quantile="0.5"} 1', char(10), '# EOF', char(10))) ORDER BY value FORMAT TSV;
