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

-- Zero-row result still terminates the OpenMetrics stream with `# EOF`.
SELECT 'm' AS name, 1.0 AS value WHERE 0 FORMAT OpenMetrics;

-- Output schema validation: `timestamp` must be `Int64` (or `Nullable(Int64)`); other numeric
-- types are rejected so the ms-as-seconds contract is unambiguous at the column level. The
-- output format is instantiated client-side under `FORMAT`, so the rejection is a `clientError`.
SELECT name, value, CAST(timestamp AS Float64) AS timestamp
FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp)
FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
SELECT name, value, CAST(timestamp AS UInt32) AS timestamp
FROM (SELECT 'm' AS name, 1.0 AS value, 0 AS timestamp)
FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }


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

-- Exact boundary round-trip: the decimal tokens that `FORMAT OpenMetrics` emits for `Int64::min`
-- and `Int64::max` must parse back to the same ms values. The parser uses exact integer/decimal
-- arithmetic so the `(seconds * 1000) + frac_ms` packs without `Float64` precision loss.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 9223372036854775.807', char(10), '# EOF', char(10))
)
FORMAT TSV;
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 -9223372036854775.808', char(10), '# EOF', char(10))
)
FORMAT TSV;
-- One ms past the boundary on either side must throw.
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 9223372036854775.808', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, 'name String, value Float64, timestamp Nullable(Int64)', concat('m 1 -9223372036854775.809', char(10))); -- { serverError INCORRECT_DATA }
-- Sub-millisecond fractional digits beyond the third are truncated, not rejected.
SELECT *
FROM format(
    OpenMetrics,
    'name String, value Float64, timestamp Nullable(Int64)',
    concat('m 1 1.7895', char(10), '# EOF', char(10))
)
FORMAT TSV;

-- `markFormatSupportsSubsetOfColumns` contract: the parser still reads `name`/`value` from
-- every line, but only inserts the columns the query actually selected. Each query below uses
-- the same single-line input and exercises a different non-full-column projection.
SELECT name
FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10)))
FORMAT TSV;
SELECT value
FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10)))
FORMAT TSV;
SELECT timestamp
FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10)))
FORMAT TSV;
SELECT labels
FROM format(OpenMetrics, concat('demo_jobs_processed_total{job="b"} 42 1.5', char(10), '# EOF', char(10)))
FORMAT TSV;

-- ----------------------------------------------------------------------------
-- Type-specific suffix folding.
--   * `_bucket` belongs only to `histogram` families. Under `# TYPE x summary`
--     a sibling `x_bucket{le=...}` line must stay as its own metric named
--     `x_bucket`, with `le` preserved as a normal label.
--   * `_sum` and `_count` are shared by `histogram` and `summary` families.
--   * Collisions between the synthesized empty marker label and a user-provided
--     non-empty `sum`/`count` label are rejected as `INCORRECT_DATA` instead of
--     silently overwriting the user value.
-- ----------------------------------------------------------------------------
-- `_bucket` under summary does NOT fold; `le` stays as a normal label.
SELECT name, value, labels, type
FROM format(
    OpenMetrics,
    concat('# TYPE x summary', char(10), 'x_bucket{le="0.5"} 3', char(10), '# EOF', char(10))
)
ORDER BY name, value FORMAT TSV;
-- `_sum` / `_count` still fold under summary.
SELECT name, value, labels, type
FROM format(
    OpenMetrics,
    concat('# TYPE x summary', char(10), 'x_sum 5', char(10), 'x_count 7', char(10), '# EOF', char(10))
)
ORDER BY name, value FORMAT TSV;
-- `_bucket` still folds under histogram.
SELECT name, value, labels, type
FROM format(
    OpenMetrics,
    concat('# TYPE x histogram', char(10), 'x_bucket{le="0.5"} 3', char(10), '# EOF', char(10))
)
ORDER BY name, value FORMAT TSV;
-- Empty `sum=""` / `count=""` markers from external producers are idempotent (no error).
SELECT name, value, labels
FROM format(
    OpenMetrics,
    concat('# TYPE x histogram', char(10), 'x_sum{sum=""} 5', char(10), 'x_count{count=""} 7', char(10), '# EOF', char(10))
)
ORDER BY name, value FORMAT TSV;
-- Collision: user-provided non-empty `sum`/`count` label on a `_sum`/`_count` sample is rejected.
SELECT * FROM format(OpenMetrics, concat('# TYPE x histogram', char(10), 'x_sum{sum="bytes"} 5', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }
SELECT * FROM format(OpenMetrics, concat('# TYPE x summary', char(10), 'x_count{count="oops"} 7', char(10), '# EOF', char(10))); -- { serverError INCORRECT_DATA }

-- ----------------------------------------------------------------------------
-- Output: `le -> _bucket` rewrite is histogram-only; summary preserves `le`.
-- Output: `sum`/`count` are suffix markers only when the label value is empty;
-- a non-empty user value is preserved as a normal label.
-- ----------------------------------------------------------------------------
-- Histogram + `{le: "0.5"}` -> `<name>_bucket{le="0.5"}`.
SELECT 'h' AS name, 3.0 AS value, '' AS help, 'histogram' AS type, map('le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;
-- Summary + `{le: "0.5"}` -> `<name>{le="0.5"}` (no `_bucket` rewrite, `le` preserved).
SELECT 's' AS name, 3.0 AS value, '' AS help, 'summary' AS type, map('le', '0.5') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;
-- Histogram + `{sum: ""}` marker -> `<name>_sum` (label dropped).
SELECT 'h' AS name, 9.0 AS value, '' AS help, 'histogram' AS type, map('sum', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;
-- Summary + `{count: ""}` marker -> `<name>_count` (label dropped).
SELECT 's' AS name, 11.0 AS value, '' AS help, 'summary' AS type, map('count', '') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;
-- Histogram + non-empty `sum` label -> kept as a normal label (no `_sum` rewrite).
SELECT 'h' AS name, 7.0 AS value, '' AS help, 'histogram' AS type, map('sum', 'bytes') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;
-- Summary + non-empty `count` label -> kept as a normal label (no `_count` rewrite).
SELECT 's' AS name, 8.0 AS value, '' AS help, 'summary' AS type, map('count', 'oops') AS labels, CAST(NULL AS Nullable(Int64)) AS timestamp, '' AS unit
FORMAT OpenMetrics;

-- ----------------------------------------------------------------------------
-- Output label serialization contract.
--   * Only `\\`, `\"`, and `\n` escapes are emitted; other control characters are
--     rejected so output round-trips through the OpenMetrics input parser.
--   * Duplicate label keys in the `labels` map are rejected (OpenMetrics requires
--     unique names; the input side already treats duplicates as `INCORRECT_DATA`).
-- ----------------------------------------------------------------------------
-- Newline is escaped as `\n` and remains readable by `FORMAT OpenMetrics` input.
SELECT 'm' AS name, 1.0 AS value, map('k', concat('a', char(10), 'b')) AS labels FORMAT OpenMetrics;
-- Tab and other unsupported control characters are rejected at output time.
SELECT 'm' AS name, 1.0 AS value, map('k', concat('a', char(9), 'b')) AS labels FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
-- Duplicate keys in the `labels` map are rejected instead of silently keeping the first.
SELECT 'm' AS name, 1.0 AS value, map('a', '1', 'a', '2') AS labels FORMAT OpenMetrics; -- { clientError BAD_ARGUMENTS }
