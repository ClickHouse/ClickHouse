-- Tags: long, no-tsan, no-asan, no-ubsan, no-msan, no-debug

-- Randomized max_insert_threads can create multiple parts, causing
-- the row count to slightly exceed max_rows_to_read due to granule-level
-- counting overhead across parts. Pin to 1 for deterministic data layout.
SET max_insert_threads = 1;

CREATE TABLE window_function_threading
Engine = MergeTree
ORDER BY (ac, nw)
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi'
AS SELECT
        toUInt64(toFloat32(number % 2) % 20000000) as ac,
        toFloat32(1) as wg,
        toUInt16(toFloat32(number % 3) % 400) as nw
FROM numbers_mt(10000000);

SELECT count() FROM (EXPLAIN PIPELINE SELECT
    nw,
    sum(WR) AS R,
    sumIf(WR, uniq_rows = 1) AS UNR
FROM
(
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    GROUP BY ac, nw
)
GROUP BY nw
ORDER BY nw ASC, R DESC
LIMIT 10) where explain ilike '%ScatterByPartitionTransform%' SETTINGS max_threads = 4;

-- { echoOn }

SELECT
    nw,
    sum(WR) AS R,
    sumIf(WR, uniq_rows = 1) AS UNR
FROM
(
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    GROUP BY ac, nw
)
GROUP BY nw
ORDER BY nw ASC, R DESC
LIMIT 10;

SELECT
    nw,
    sum(WR) AS R,
    sumIf(WR, uniq_rows = 1) AS UNR
FROM
(
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    GROUP BY ac, nw
)
GROUP BY nw
ORDER BY nw ASC, R DESC
LIMIT 10
SETTINGS max_threads = 1;

SET max_rows_to_read = 50000000;

SELECT
    nw,
    sum(WR) AS R,
    sumIf(WR, uniq_rows = 1) AS UNR
FROM
(
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    WHERE (ac % 4) = 0
    GROUP BY
        ac,
        nw
    UNION ALL
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    WHERE (ac % 4) = 1
    GROUP BY
        ac,
        nw
    UNION ALL
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    WHERE (ac % 4) = 2
    GROUP BY
        ac,
        nw
    UNION ALL
    SELECT
        uniq(nw) OVER (PARTITION BY ac) AS uniq_rows,
        AVG(wg) AS WR,
        ac,
        nw
    FROM window_function_threading
    WHERE (ac % 4) = 3
    GROUP BY
        ac,
        nw
)
GROUP BY nw
ORDER BY nw ASC, R DESC
LIMIT 10;
