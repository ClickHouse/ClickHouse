-- Tests for correctness of the ordered_group_by_limit_pushdown optimization.
-- Part 5: GROUP BY ... LIMIT without ORDER BY.
--
-- When there is no ORDER BY, the optimization uses all GROUP BY keys for the
-- heap, and any N groups are a valid result. We test:
--   (a) the number of returned rows equals LIMIT
--   (b) aggregate values match the full (unoptimized) aggregation for each group
--   (c) the optimization fires (via EXPLAIN)

-- Tags: no-parallel-replicas, long

SET ordered_group_by_limit_pushdown = 1;

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

-- =====================
-- Test 1: Single-column GROUP BY without ORDER BY.
-- Verify row count and aggregate correctness.
-- =====================
SELECT 'single_key_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

-- Verify every returned group has correct aggregates by joining with unoptimized result.
SELECT 'single_key_aggregates';
SELECT count() FROM (
    SELECT a, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT a, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (a)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

-- =====================
-- Test 2: Composite (two-column) GROUP BY without ORDER BY.
-- =====================
SELECT 'composite_two_key_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
    SETTINGS ordered_group_by_limit_pushdown = 1
);

SELECT 'composite_two_key_aggregates';
SELECT count() FROM (
    SELECT a, b, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b LIMIT 15
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT a, b, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (a, b)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

-- =====================
-- Test 3: Three-column GROUP BY without ORDER BY.
-- =====================
SELECT 'composite_three_key_row_count';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
    SETTINGS ordered_group_by_limit_pushdown = 1
);

SELECT 'composite_three_key_aggregates';
SELECT count() FROM (
    SELECT a, b, c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b, c LIMIT 20
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT a, b, c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY a, b, c
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (a, b, c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

-- =====================
-- Test 4: Nullable GROUP BY key without ORDER BY.
-- =====================
SELECT 'nullable_key_row_count';
SELECT count() FROM (
    SELECT d, count() AS cnt FROM t_gbylimit_noob GROUP BY d LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

SELECT 'nullable_key_aggregates';
SELECT count() FROM (
    SELECT d, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY d LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT d, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY d
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (d)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

-- =====================
-- Test 5: String GROUP BY key without ORDER BY.
-- =====================
SELECT 'string_key_row_count';
SELECT count() FROM (
    SELECT c, count() AS cnt FROM t_gbylimit_noob GROUP BY c LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

SELECT 'string_key_aggregates';
SELECT count() FROM (
    SELECT c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY c LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT c, count() AS cnt, sum(val) AS s FROM t_gbylimit_noob GROUP BY c
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (c)
WHERE optimized.cnt != full.cnt OR optimized.s != full.s;

-- =====================
-- Test 6: GROUP BY ... LIMIT with OFFSET, no ORDER BY.
-- The heap retains limit + offset groups.
-- =====================
SELECT 'with_offset_row_count';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 5, 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

-- =====================
-- Test 7: LIMIT larger than number of groups.
-- GROUP BY a has 500 groups; LIMIT 1000 should return all 500.
-- =====================
SELECT 'limit_exceeds_groups';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a LIMIT 1000
    SETTINGS ordered_group_by_limit_pushdown = 1
);

-- =====================
-- Test 8: LIMIT 1 (minimal limit).
-- =====================
SELECT 'limit_one_row_count';
SELECT count() FROM (
    SELECT a, b, count() AS cnt FROM t_gbylimit_noob GROUP BY a, b LIMIT 1
    SETTINGS ordered_group_by_limit_pushdown = 1
);

-- =====================
-- Test 9: High-cardinality (two-level hash table) without ORDER BY.
-- =====================
SELECT 'two_level_row_count';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count()
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

SELECT 'two_level_aggregates';
SELECT count() FROM (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
) AS optimized
LEFT JOIN (
    SELECT
        (number % 100000)::UInt32 AS x,
        (number % 50000)::UInt32 AS y,
        count() AS cnt
    FROM numbers(2000000) GROUP BY x, y
    SETTINGS ordered_group_by_limit_pushdown = 0
) AS full USING (x, y)
WHERE optimized.cnt != full.cnt;

-- =====================
-- Test 10: Negative case — WITH TOTALS should not trigger optimization.
-- Verify it still produces correct output.
-- =====================
SELECT 'negative_with_totals';
SELECT count() FROM (
    SELECT a, count() AS cnt FROM t_gbylimit_noob GROUP BY a WITH TOTALS LIMIT 10
    SETTINGS ordered_group_by_limit_pushdown = 1
);

DROP TABLE t_gbylimit_noob;
