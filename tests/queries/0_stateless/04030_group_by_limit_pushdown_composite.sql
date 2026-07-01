-- Correctness of enable_group_by_top_k_optimization for composite GROUP BY keys and ORDER BY prefix matching.

-- Tags: no-parallel-replicas, long

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;

DROP TABLE IF EXISTS t_gbylimit_comp;

CREATE TABLE t_gbylimit_comp
(
    a UInt32,
    b UInt32,
    c String,
    d Nullable(UInt32),
    val UInt64
) ENGINE = MergeTree ORDER BY a;

INSERT INTO t_gbylimit_comp
SELECT
    (number % 500)::UInt32,
    (number % 200)::UInt32,
    toString(number % 300),
    if(number % 97 = 0, NULL, (number % 400)::UInt32),
    number
FROM numbers(20000);

SELECT 'composite_two_int';
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b ORDER BY a, b ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_int_string';
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, c ORDER BY a, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_three_keys';
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, b, c, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b, c ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a, d ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'prefix_one_of_two';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b ASC;

SELECT 'prefix_one_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b, c ASC;

SELECT 'prefix_two_of_three';
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b, c ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, c, count() AS cnt
    FROM t_gbylimit_comp GROUP BY a, b, c ORDER BY a, b ASC LIMIT 12
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b, c ASC;

SELECT 'prefix_with_offset';
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY a, b ASC
EXCEPT
SELECT * FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s
    FROM t_gbylimit_comp GROUP BY a, b ORDER BY a ASC LIMIT 4, 6
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY a, b ASC;

SELECT 'composite_two_level';
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT
    (number % 100000)::UInt32 AS x,
    (number % 50000)::UInt32 AS y,
    count()
FROM numbers(200000) GROUP BY x, y ORDER BY x, y ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'prefix_two_level';
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 1
) ORDER BY x, y ASC
EXCEPT
SELECT * FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(200000) GROUP BY x, y ORDER BY x ASC LIMIT 10
    SETTINGS enable_group_by_top_k_optimization = 0
) ORDER BY x, y ASC;

SELECT 'nullable_nulls_first_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_last_asc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d ASC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_first_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS FIRST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'nullable_nulls_last_desc';
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY d ORDER BY d DESC NULLS LAST LIMIT 5
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable_nulls_first';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS FIRST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'composite_nullable_nulls_last';
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT a, d, count(), sum(val)
FROM t_gbylimit_comp GROUP BY a, d ORDER BY a ASC, d ASC NULLS LAST LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit_comp;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
