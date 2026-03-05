-- Tests for correctness of the ordered_group_by_limit_pushdown optimization.
-- Part 3: edge cases, negative tests, and two-level hash table conversion.

-- Tags: no-parallel-replicas, long, no-sanitizers

SET ordered_group_by_limit_pushdown = 1;
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

-- =====================
-- Test with LIMIT offset (LIMIT 5, 10)
-- =====================
SELECT 'limit_with_offset';
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u64, count(), sum(val)
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 ASC LIMIT 5, 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test with multiple aggregate functions
-- =====================
SELECT 'multiple_aggregates';
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count(), sum(val), min(val), max(val), avg(val)
FROM t_gbylimit GROUP BY k_u32 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Negative tests: optimization must NOT be applied in these cases.
-- We verify that results still match (the optimization gracefully disables).
-- =====================

-- DESC order (not supported yet)
SELECT 'negative_desc';
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u64, count()
FROM t_gbylimit GROUP BY k_u64 ORDER BY k_u64 DESC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- WITH TOTALS (overflow_row): the optimization should NOT be applied.
-- Totals rows propagate through subqueries, so we use a materializing
-- CTE to strip them before comparing.
SELECT 'negative_with_totals';
WITH
    a AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS ordered_group_by_limit_pushdown = 1),
    b AS (SELECT k_u32, count() AS cnt FROM t_gbylimit GROUP BY k_u32 WITH TOTALS ORDER BY k_u32 ASC LIMIT 10 SETTINGS ordered_group_by_limit_pushdown = 0)
SELECT count() FROM (SELECT * FROM a EXCEPT SELECT * FROM b);

-- HAVING (FilterStep breaks pattern)
SELECT 'negative_having';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 HAVING cnt > 1 ORDER BY k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- ORDER BY aggregate (not GROUP BY key): add tie-breaker for determinism
SELECT 'negative_order_by_aggregate';
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, count() AS cnt
FROM t_gbylimit GROUP BY k_u32 ORDER BY cnt DESC, k_u32 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- Multiple GROUP BY keys
SELECT 'negative_multi_key';
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT k_u32, k_u64, count()
FROM t_gbylimit GROUP BY k_u32, k_u64 ORDER BY k_u32, k_u64 ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test two-level hash table conversion (happens with high cardinality).
-- The bounded heap must survive the single-to-two-level migration.
-- =====================
SELECT 'two_level';
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT number, count()
FROM numbers(2000000) GROUP BY number ORDER BY number ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

-- =====================
-- Test two-level with String key
-- =====================
SELECT 'two_level_string';
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 1
EXCEPT
SELECT toString(number) AS k, count()
FROM numbers(2000000) GROUP BY k ORDER BY k ASC LIMIT 10
SETTINGS ordered_group_by_limit_pushdown = 0;

DROP TABLE t_gbylimit;
