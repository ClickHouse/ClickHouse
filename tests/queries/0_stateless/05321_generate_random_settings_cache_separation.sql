-- Calls to `generateRandom` with identical positional arguments keep their own `SETTINGS`.
SELECT label, nulls, rows
FROM
(
    SELECT 0 AS label, countIf(isNull(x)) AS nulls, count() AS rows
    FROM (SELECT x FROM generateRandom('x Nullable(UInt64)', 42, 4, 2, SETTINGS null_ratio = 0) LIMIT 100)
    UNION ALL
    SELECT 1 AS label, countIf(isNull(x)) AS nulls, count() AS rows
    FROM (SELECT x FROM generateRandom('x Nullable(UInt64)', 42, 4, 2, SETTINGS null_ratio = 1) LIMIT 100)
)
ORDER BY label
SETTINGS max_threads = 1;

-- Reverse the arms so that either setting can be the first one seen by the table function cache.
SELECT label, nulls, rows
FROM
(
    SELECT 1 AS label, countIf(isNull(x)) AS nulls, count() AS rows
    FROM (SELECT x FROM generateRandom('x Nullable(UInt64)', 42, 4, 2, SETTINGS null_ratio = 1) LIMIT 100)
    UNION ALL
    SELECT 0 AS label, countIf(isNull(x)) AS nulls, count() AS rows
    FROM (SELECT x FROM generateRandom('x Nullable(UInt64)', 42, 4, 2, SETTINGS null_ratio = 0) LIMIT 100)
)
ORDER BY label
SETTINGS max_threads = 1;
