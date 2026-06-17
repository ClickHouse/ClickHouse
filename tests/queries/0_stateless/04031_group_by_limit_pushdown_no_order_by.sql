-- Correctness of enable_group_by_top_k_optimization for GROUP BY ... LIMIT without ORDER BY.
-- Without ORDER BY any N groups are valid, so we check row count and per-group aggregate values.

-- Tags: no-parallel-replicas, long

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_noob;

CREATE TABLE t_gbylimit_noob
(
    a UInt32,
    b UInt32,
    c String,
    d Nullable(UInt32),
    val UInt64
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_gbylimit_noob
SELECT
    (number % 500)::UInt32,
    (number % 200)::UInt32,
    toString(number % 300),
    if(number % 97 = 0, NULL, (number % 400)::UInt32),
    number
FROM numbers(100000);

SELECT 'single_key_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'single_key_aggregates';
SELECT count() FROM (
    SELECT a, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT a, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (a)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'composite_two_key_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'composite_two_key_aggregates';
SELECT count() FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT a, b, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (a, b)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'composite_three_key_row_count';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'composite_three_key_aggregates';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT a, b, c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b, c
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (a, b, c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'nullable_key_row_count';
SELECT count() FROM (
    SELECT d, count() AS cnt FROM t_gbylimit_noob GROUP BY d LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'nullable_key_aggregates';
-- IS NOT DISTINCT FROM is used in the join because USING/= treats NULL != NULL.
SELECT count() FROM (
    SELECT d, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY d LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT d, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY d
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full ON optimized.d IS NOT DISTINCT FROM full.d
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'string_key_row_count';
SELECT count() FROM (
    SELECT c, count() AS cnt FROM t_gbylimit_noob GROUP BY c LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'string_key_aggregates';
SELECT count() FROM (
    SELECT c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY c LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY c
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

SELECT 'with_offset_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 5, 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

-- GROUP BY a has 500 groups; LIMIT 1000 should return all 500.
SELECT 'limit_exceeds_groups';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 1000
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'limit_one_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 1
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'two_level_row_count';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count()
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

SELECT 'two_level_aggregates';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) AS optimized
LEFT JOIN (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y
    SETTINGS enable_group_by_top_k_optimization = 0
) AS full USING (x, y)
WHERE optimized.cnt != full.cnt;

-- Negative case: WITH TOTALS should not trigger the optimization.
SELECT 'negative_with_totals';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a WITH TOTALS LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
);

DROP TABLE t_gbylimit_noob;

-- Guard against the environment silently disabling the optimization.
-- The trivial analyzer pass and external aggregation are disabled so Pattern 2 is exercised.
SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k LIMIT 5 SETTINGS optimize_trivial_group_by_limit_query = 0, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0) WHERE explain LIKE '%Top-K%';
