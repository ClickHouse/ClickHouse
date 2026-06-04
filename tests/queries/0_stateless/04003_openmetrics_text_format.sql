-- ============================================================================
-- FORMAT OpenMetrics: output round-trip / sample shapes / metadata.
-- The `timestamp` column stays Prometheus-compatible milliseconds; the writer
-- divides by 1000 to emit OpenMetrics seconds and the reader multiplies back.
-- ============================================================================

SELECT 'http_requests_total' AS name, 1. AS value, '' AS help, '' AS type, CAST(map(), 'Map(String, String)') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;

-- Output: 100 ms in the ClickHouse column emits as `0.1` seconds in OpenMetrics text.
SELECT 'http_requests_total' AS name, 2. AS value, 'Total number of HTTP requests' AS help, 'counter' AS type, map('method', 'GET', 'status', '200') AS labels, CAST(100 AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;

-- Output: ms-to-seconds boundary cases (trailing-zero stripping; negative-zero seconds; INT64_MIN).
SELECT 'a' AS name, 1.0 AS value, CAST(1520879607789 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'b' AS name, 2.0 AS value, CAST(1520879607000 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'c' AS name, 3.0 AS value, CAST(-500 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
SELECT 'd' AS name, 4.0 AS value, CAST(0 AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;
-- INT64_MIN written via bit math to avoid the literal being parsed as `-(UInt64{2^63})`.
SELECT 'e' AS name, 5.0 AS value, CAST(bitShiftLeft(toInt64(1), 63) AS Nullable(Int64)) AS timestamp FORMAT OpenMetrics;

-- Output schema validation: `timestamp` must be `Int64` (or `Nullable(Int64)`); other numeric
-- types are rejected so the ms-as-seconds contract is unambiguous at the column level.
SELECT name, value, CAST(timestamp AS Float64) AS timestamp
FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp)
FORMAT OpenMetrics; -- { serverError BAD_ARGUMENTS }
SELECT name, value, CAST(timestamp AS UInt32) AS timestamp
FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp)
FORMAT OpenMetrics; -- { serverError BAD_ARGUMENTS }


-- ============================================================================
-- FORMAT OpenMetrics: input parsing and validation.
-- ============================================================================

-- Input: OpenMetrics timestamp tokens are epoch seconds; reader multiplies by 1000
-- before storing in the ClickHouse `Nullable(Int64)` column (fractional ms preserved).
-- `999` seconds -> 999000 ms; `42` is the sample value.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
$$
# HELP demo_jobs_processed_total Number of completed batch jobs
# TYPE demo_jobs_processed_total counter
# UNIT demo_jobs_processed_total seconds
demo_jobs_processed_total 42 999
# EOF
$$
)
FORMAT TSV;

-- Tab is valid whitespace between metric descriptor and value (OpenMetrics / Prometheus text).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('ingress_http_requests_total', char(9), '7', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Escaped newline and quote inside label value (\\n, \", \\ per exposition format).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('demo_log_lines_total{k="a', char(92), 'n', 'b"} 1', char(10), '# EOF', char(10))
)
FORMAT TSV;

SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('demo_error_messages_total{k="a', char(92), '"', 'b"} 1', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Reject trailing garbage after sample value / timestamp (no silent truncation).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 abc', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 2 extra', char(10))); -- { serverError INCORRECT_DATA }

-- Reject `#` tails that are not a valid exemplar (`# {labels} <value>`).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 # not_an_exemplar', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 # {broken', char(10))); -- { serverError INCORRECT_DATA }

-- Valid exemplar suffix is accepted (labels are not ingested into the row schema).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64',
    concat('demo_http_requests_total 1 # {trace_id="abc"} 0.5', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Exemplar may include an optional timestamp after exemplar value.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64',
    concat('foo_bucket{le="10"} 17 # {trace_id="x"} 9.8 1520879607.789', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Reject whitespace between metric name and `{labels}`.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('metric {job="x"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- Input: OpenMetrics realnumber timestamp tokens (`+2`, fractional `1520879607.789`).
-- Stored value is `token_seconds * 1000` ms.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 +2', char(10), '# EOF', char(10))
)
FORMAT TSV;

SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 1520879607.789', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Input: decimal token below 1 second (e.g. `-.5`) preserves negative sign as `-500` ms.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 -.5', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- Reject empty metric name and duplicate label keys (invalid exposition text).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('{k="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{a="1",a="2"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- Descriptor and value must be separated by ASCII whitespace (no `m{job="x"}1`).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{job="x"}1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject malformed label names (empty key, whitespace in key, `:` in key).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{a ="v"} 1', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m{trace:id="x"} 1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject non-finite timestamp tokens even when the schema has no timestamp column.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 NaN', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 +Inf', char(10))); -- { serverError INCORRECT_DATA }

-- `unit` must be String for OpenMetrics output schema validation.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, unit UInt8', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- Reject malformed float sample tokens (no partial parse, e.g. 1abc must not become 1).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1abc', char(10))); -- { serverError INCORRECT_DATA }

-- Reject trailing payload after logical end (# EOF).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF', char(10), 'm 1', char(10))); -- { serverError INCORRECT_DATA }

-- Reject non-whitespace on the same line after `# EOF` (not a valid logical terminator).
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('# EOF trailing', char(10))); -- { serverError INCORRECT_DATA }

-- Reject malformed timestamp token (`-` without digits) even when the schema has no timestamp column.
SELECT * FROM format(OpenMetrics, 'name String, value Float64', concat('m 1 -', char(10))); -- { serverError INCORRECT_DATA }

-- Reject incompatible declared column types (validated up front like Prometheus output format).
SELECT * FROM format(OpenMetrics, 'name String, value Float64, help UInt64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- Schema must match insertion column types (non-nullable Float64; String not FixedString) — avoid release assert_cast UB.
SELECT * FROM format(OpenMetrics, 'name String, value Nullable(Float64)', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }
SELECT * FROM format(OpenMetrics, 'name FixedString(16), value Float64', concat('x 1', char(10))); -- { serverError BAD_ARGUMENTS }

-- Do not fold `..._sum` / `..._count` into the base name unless # TYPE is histogram or summary (counter/gauge can legitimately use those suffixes).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('# HELP http_requests_total help\n', '# TYPE http_requests_total counter\n', 'http_requests_total_sum 1\n', '# EOF\n')
)
FORMAT TSV;

-- Histogram/summary `_sum` still maps to the family with synthetic `sum` label.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('# HELP http_request_duration_seconds help\n', '# TYPE http_request_duration_seconds histogram\n', 'http_request_duration_seconds_sum 5\n', '# EOF\n')
)
FORMAT TSV;

-- `# TYPE` must not treat trailing spaces as part of the type token (histogram/summary normalization).
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, help String, type String, labels Map(String, String), timestamp Nullable(Int64), unit String',
    concat('# HELP rpc_duration_seconds help\n', '# TYPE rpc_duration_seconds histogram   \n', 'rpc_duration_seconds_bucket{le="0.5"} 3\n', '# EOF\n')
)
FORMAT TSV;

-- Range guards (token is seconds, stored as `seconds * 1000` ms):
-- Largest in-range integer seconds (INT64_MAX / 1000) is accepted; +1 second overflows Int64 ms.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 9223372036854775', char(10), '# EOF', char(10))
)
FORMAT TSV;
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854776', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -9223372036854776', char(10))); -- { serverError INCORRECT_DATA }
-- Float-shaped tokens that obviously overflow Int64 ms after the *1000 conversion are also rejected.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 1e20', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -1e20', char(10))); -- { serverError INCORRECT_DATA }
