SET max_threads = 4;
SET max_block_size = 100;
SET enable_parallel_replicas = 0;
SET allow_parallel_distinct = 1;
SET allow_distinct_partitions_independently = 0;
SET allow_aggregate_partitions_independently = 1;
SET max_rows_to_group_by = 0;
SET max_rows_in_distinct = 0;
SET max_bytes_in_distinct = 0;

-- A union combines independently partitioned inputs and requires merging the outer aggregation.
SELECT count(), sum(c), min(c), max(c) FROM
(
SELECT k, count() AS c FROM
(
    SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)
    UNION ALL
    SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)
)
GROUP BY k
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM
(
EXPLAIN actions = 1
SELECT k, count() AS c FROM
(
    SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)
    UNION ALL
    SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)
)
GROUP BY k
);

-- Each branch can reuse its own partitioning without passing it to its siblings or the union.
SELECT count(), sum(c), min(c), max(c) FROM
(
SELECT k, sum(c) AS c FROM
(
    SELECT k, count() AS c FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) GROUP BY k
    UNION ALL
    SELECT number % 10 AS k, count() AS c FROM numbers_mt(10000) GROUP BY k
)
GROUP BY k
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 1 FROM
(
EXPLAIN actions = 1
SELECT k, sum(c) AS c FROM
(
    SELECT k, count() AS c FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) GROUP BY k
    UNION ALL
    SELECT number % 10 AS k, count() AS c FROM numbers_mt(10000) GROUP BY k
)
GROUP BY k
);

-- A join prevents either input partitioning from being reused by the outer aggregation.
SELECT count(), sum(c), min(c), max(c) FROM
(
SELECT k, count() AS c FROM
(
    SELECT k FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) ARRAY JOIN [0, 1] AS x
) AS l
INNER JOIN
(
    SELECT k FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) ARRAY JOIN [0, 1] AS y
) AS r USING k
GROUP BY k
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM
(
EXPLAIN actions = 1
SELECT k, count() AS c FROM
(
    SELECT k FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) ARRAY JOIN [0, 1] AS x
) AS l
INNER JOIN
(
    SELECT k FROM (SELECT DISTINCT number % 10 AS k FROM numbers_mt(10000)) ARRAY JOIN [0, 1] AS y
) AS r USING k
GROUP BY k
);

-- Constant scatter keys add no column dependencies to a branch's partitioning expression.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT k, count() AS c
    FROM (SELECT DISTINCT number % 10 AS k, 1 AS v FROM numbers_mt(10000))
    ARRAY JOIN [0, 1] AS x
    GROUP BY k
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 1 FROM
(
    EXPLAIN actions = 1
    SELECT k, count() AS c
    FROM (SELECT DISTINCT number % 10 AS k, 1 AS v FROM numbers_mt(10000))
    ARRAY JOIN [0, 1] AS x
    GROUP BY k
);

-- A constant grouping key does not determine the variable scatter keys.
SELECT count(), sum(c), min(c), max(c) FROM
(
    SELECT v, count() AS c
    FROM (SELECT DISTINCT number % 10 AS k, 1 AS v FROM numbers_mt(10000))
    ARRAY JOIN [0, 1] AS x
    GROUP BY v
);
SELECT countIf(explain LIKE '%Skip merging: 1%') = 0 FROM
(
    EXPLAIN actions = 1
    SELECT v, count() AS c
    FROM (SELECT DISTINCT number % 10 AS k, 1 AS v FROM numbers_mt(10000))
    ARRAY JOIN [0, 1] AS x
    GROUP BY v
);
