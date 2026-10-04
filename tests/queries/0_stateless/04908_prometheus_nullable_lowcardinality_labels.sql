-- Regression test for the `Prometheus` output format `labels` column when
-- `optimize_if_transform_const_strings_to_lowcardinality` produces `LowCardinality(String)`
-- map values.
--
-- Concern: the `labels` type check unwraps `Nullable`, so a hypothetical
-- `Nullable(Map(String, LowCardinality(String)))` could pass validation and then be
-- silently dropped by the writer (which only handles a bare `ColumnMap`).
--
-- In practice `Nullable(Map(...))` is not a constructible type, and an expression like
-- `if(cond, map(...), NULL)` resolves to `Variant(Map(...))`, which the format rejects
-- explicitly instead of silently emitting a metric without labels. This test pins that
-- behavior down.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;

-- A `Map` whose value is `LowCardinality(String)` (from an optimized `if` branch) is accepted.
SELECT
    'http_requests_total' AS name,
    'counter' AS type,
    '' AS help,
    map('status', if(number = 0, 'ok', 'bad')) AS labels,
    number AS value,
    0 :: Float64 AS timestamp
FROM numbers(2)
ORDER BY value
FORMAT Prometheus;

-- A "nullable" labels expression resolves to `Variant(Map(String, String))`,
-- which the format must reject rather than serialize a metric without labels.
-- `clickhouse-client` applies the `Prometheus` output format locally to the `Native`
-- blocks received from the server, so the rejection surfaces as a `clientError`.
SELECT
    'http_requests_total' AS name,
    'counter' AS type,
    '' AS help,
    if(number % 2 = 0, map('status', if(number = 0, 'ok', 'bad')), NULL) AS labels,
    number AS value,
    0 :: Float64 AS timestamp
FROM numbers(4)
ORDER BY value
FORMAT Prometheus; -- { clientError BAD_ARGUMENTS }
