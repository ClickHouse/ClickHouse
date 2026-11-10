-- https://github.com/ClickHouse/ClickHouse/issues/80495
SET enable_analyzer=1;
SELECT DISTINCT
    1 AS a,
    arrayMap(x -> intDiv(x, a), [1, 2, 3])
GROUP BY
    *,
    2,
    2,
    *,
    isNullable(2) IS NOT NULL
WITH CUBE
SETTINGS max_threads = 1, group_by_use_nulls = true;