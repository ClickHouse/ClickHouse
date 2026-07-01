-- Correctness of enable_group_by_top_k_optimization: edge cases, negative tests, two-level hash table conversion.

-- Tags: no-parallel-replicas, long, no-sanitizers

-- CI profile sets max_rows_to_group_by, which disables the optimization; reset it.
SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;

SET enable_group_by_top_k_optimization = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_gbylimit;

CREATE TABLE t_gbylimit
(
    k_u32 UInt32,
    k_u64 UInt64,
    val UInt64
) ENGINE = MergeTree ORDER BY k_u64;

INSERT INTO t_gbylimit
SELECT
    (number * 7 + 13) % 40000,
    number,
    number
FROM numbers(50000);

SELECT 'limit_with_offset';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'multiple_aggregates';
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'desc_order';
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'negative_with_totals';
WITH
    a AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 1),
    b AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS enable_group_by_top_k_optimization = 0)
SELECT count() FROM (SELECT * FROM a EXCEPT SELECT * FROM b);

SELECT 'negative_having';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'negative_order_by_aggregate';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'negative_multi_key';
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'two_level';
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

SELECT 'two_level_string';
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 1
EXCEPT
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS enable_group_by_top_k_optimization = 0;

DROP TABLE t_gbylimit;

SELECT 'optimization_applied_guard';
SELECT count() FROM (EXPLAIN actions = 1 SELECT number AS k FROM numbers(100) GROUP BY k ORDER BY k LIMIT 5) WHERE explain LIKE '%Top-K%';
