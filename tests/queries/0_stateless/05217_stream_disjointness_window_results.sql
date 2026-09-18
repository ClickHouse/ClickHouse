SET max_threads = 4;
SET max_block_size = 100;
SET enable_parallel_replicas = 0;
SET allow_parallel_distinct = 1;
SET allow_distinct_partitions_independently = 0;
SET allow_aggregate_partitions_independently = 1;
SET allow_window_partitions_independently = 1;
SET query_plan_enable_multithreading_after_window_functions = 0;
SET max_rows_to_group_by = 0;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- Partition keys remain usable alongside multiple window result columns.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT k, rn, s, count() AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
    GROUP BY k, rn, s
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 1 FROM
(
    EXPLAIN actions = 1
    SELECT k, rn, s, count() AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
    GROUP BY k, rn, s
);

-- Window result columns alone do not determine the input partition.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT rn, s, count() AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
    GROUP BY rn, s
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM
(
    EXPLAIN actions = 1
    SELECT rn, s, count() AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
    GROUP BY rn, s
);

-- Renaming a window result to an earlier partition key does not recover that key.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT k, count() AS c
    FROM
    (
        SELECT rn AS k
        FROM
        (
            SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
            FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
            ARRAY JOIN [0, 1] AS x
            WINDOW w AS (PARTITION BY k ORDER BY x)
        )
    )
    GROUP BY k
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM
(
    EXPLAIN actions = 1
    SELECT k, count() AS c
    FROM
    (
        SELECT rn AS k
        FROM
        (
            SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
            FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
            ARRAY JOIN [0, 1] AS x
            WINDOW w AS (PARTITION BY k ORDER BY x)
        )
    )
    GROUP BY k
);

-- A subsequent window repartitions by a result of the preceding window.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT rn, count() OVER (PARTITION BY rn) AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
);
SELECT countIf(explain LIKE '%ScatterByPartitionTransform%') = 2 FROM
(
    EXPLAIN PIPELINE
    SELECT rn, count() OVER (PARTITION BY rn) AS c
    FROM
    (
        SELECT k, row_number() OVER w AS rn, sum(x) OVER w AS s
        FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000))
        ARRAY JOIN [0, 1] AS x
        WINDOW w AS (PARTITION BY k ORDER BY x)
    )
);
