-- Regression test: a `Map` label value produced by an optimized `if` branch is typed
-- `Map(String, LowCardinality(String))` under `optimize_if_transform_const_strings_to_lowcardinality`.
-- The `Prometheus` output format must accept it instead of throwing
-- `Illegal type ... of column 'labels'`.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;

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

-- Also exercise a LowCardinality key.
SELECT
    'http_requests_total' AS name,
    'counter' AS type,
    '' AS help,
    map(if(number = 0, 'a', 'b'), 'x') AS labels,
    number AS value,
    0 :: Float64 AS timestamp
FROM numbers(2)
ORDER BY value
FORMAT Prometheus;
