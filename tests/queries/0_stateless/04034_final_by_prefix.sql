-- Test prefix FINAL BY: FINAL BY specifies fewer expressions than sorting key
-- columns. Trailing sorting key columns are implicitly collapsed (merged together).

SET enable_analyzer = 1;

-- ============================================================
-- Part 1: Basic prefix — 3-column key, FINAL BY 2 columns (identity)
-- ORDER BY (a, b, c), FINAL BY a, b → collapse `c`
-- ============================================================
SELECT '--- Part 1: identity prefix (3 cols, FINAL BY 2) ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    c UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b, c);

-- 8 rows: a in {0,1}, b in {0,1}, c in {0,1}
-- Each inserted twice with sums 1 and 2.
INSERT INTO t_prefix_final
    SELECT
        intDiv(number, 4) AS a,
        intDiv(number % 4, 2) AS b,
        number % 2 AS c,
        sumState(toUInt64(1))
    FROM numbers(8) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT
        intDiv(number, 4) AS a,
        intDiv(number % 4, 2) AS b,
        number % 2 AS c,
        sumState(toUInt64(2))
    FROM numbers(8) GROUP BY number;

-- FINAL BY a, b: merges all `c` values within each (a, b) group.
-- 4 groups: (0,0), (0,1), (1,0), (1,1), each with 2 values of c.
-- Total per group: 2 * (1 + 2) = 6.
SELECT
    a, b,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY a, b
ORDER BY a ASC, b ASC;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 2: Prefix with coarsening — 3-column key, FINAL BY 1 expression (non-identity)
-- ORDER BY (a, b, c), FINAL BY intDiv(a, 10) → collapse b and c
-- ============================================================
SELECT '--- Part 2: non-identity prefix (3 cols, FINAL BY 1) ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    c UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b, c);

-- 30 rows: a in 0..29, b=0, c=0
INSERT INTO t_prefix_final
    SELECT number, 0, 0, sumState(toUInt64(1)) FROM numbers(30) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT number, 0, 0, sumState(toUInt64(2)) FROM numbers(30) GROUP BY number;

-- FINAL BY intDiv(a, 10): merges into 3 groups (0, 1, 2).
-- Group 0: a=0..9, 10 rows * (1+2) = 30
-- Group 1: a=10..19, 10 rows * (1+2) = 30
-- Group 2: a=20..29, 10 rows * (1+2) = 30
SELECT
    intDiv(a, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY intDiv(a, 10)
ORDER BY bucket ASC;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 3: SummingMergeTree with prefix
-- ORDER BY (region, ts, detail), FINAL BY region, intDiv(ts, 10)
-- ============================================================
SELECT '--- Part 3: SummingMergeTree prefix ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    region String,
    ts UInt64,
    detail UInt64,
    val UInt64
)
ENGINE = SummingMergeTree
ORDER BY (region, ts, detail);

INSERT INTO t_prefix_final SELECT 'us', number, number % 3, 1 FROM numbers(30);
INSERT INTO t_prefix_final SELECT 'us', number, number % 3, 2 FROM numbers(30);
INSERT INTO t_prefix_final SELECT 'eu', number, number % 3, 1 FROM numbers(20);

-- FINAL BY region, intDiv(ts, 10): collapse `detail` and coarsen `ts`.
-- 'eu': 2 buckets (0,1), 10 rows each * 1 = 10 per bucket
-- 'us': 3 buckets (0,1,2), 10 rows each * (1+2) = 30 per bucket
SELECT
    region,
    intDiv(ts, 10) AS bucket,
    val AS total
FROM t_prefix_final FINAL BY region, intDiv(ts, 10)
ORDER BY region ASC, bucket ASC;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 4: Prefix FINAL BY with LIMIT pushdown
-- ORDER BY (a, b), FINAL BY a
-- ORDER BY a ASC LIMIT 2
-- ============================================================
SELECT '--- Part 4: prefix with LIMIT pushdown ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

INSERT INTO t_prefix_final
    SELECT intDiv(number, 10), number % 10, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT intDiv(number, 10), number % 10, sumState(toUInt64(2)) FROM numbers(50) GROUP BY number;

-- FINAL BY a: 5 groups (a=0..4), each with 10 values of b.
-- Total per group: 10 * (1+2) = 30.
-- LIMIT 2 should be pushed down to the merge algorithm.
SELECT
    a,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY a
ORDER BY a ASC
LIMIT 2;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 5: Prefix FINAL BY with PREWHERE
-- ORDER BY (token, ts, detail), FINAL BY token, ts
-- PREWHERE token = 'BTC'
-- ============================================================
SELECT '--- Part 5: prefix with PREWHERE ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    token String,
    ts UInt64,
    detail UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (token, ts, detail);

INSERT INTO t_prefix_final
    SELECT 'BTC', number, number % 3, sumState(toUInt64(1)) FROM numbers(30) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT 'BTC', number, number % 3, sumState(toUInt64(2)) FROM numbers(30) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT 'ETH', number, number % 2, sumState(toUInt64(1)) FROM numbers(20) GROUP BY number;

-- FINAL BY token, ts: collapse `detail`, identity on token and ts.
-- PREWHERE token = 'BTC': only BTC rows.
-- 30 unique ts values, each with up to 3 detail values, inserted twice.
-- Each (token, ts) group has up to 3 rows merged together.
-- Row ts=0: detail in {0}, 1 row, sum = 1+2 = 3
-- Row ts=1: detail in {1}, 1 row, sum = 1+2 = 3
-- ...each ts has exactly 1 detail value (number % 3), so total = 1*(1+2) = 3
SELECT
    ts,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY token, ts
PREWHERE token = 'BTC'
ORDER BY ts ASC
LIMIT 5;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 6: Prefix FINAL BY with do_not_merge_across_partitions
-- ORDER BY (a, b), FINAL BY a
-- Partitioned table with multiple partitions
-- ============================================================
SELECT '--- Part 6: prefix with partitions ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    part_key UInt64,
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY part_key
ORDER BY (a, b);

INSERT INTO t_prefix_final
    SELECT 1, intDiv(number, 5), number % 5, sumState(toUInt64(1)) FROM numbers(15) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT 1, intDiv(number, 5), number % 5, sumState(toUInt64(2)) FROM numbers(15) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT 2, intDiv(number, 5), number % 5, sumState(toUInt64(10)) FROM numbers(15) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT 2, intDiv(number, 5), number % 5, sumState(toUInt64(20)) FROM numbers(15) GROUP BY number;

-- FINAL BY a: collapse b, within each partition.
-- Each partition has 3 groups (a=0..2), each with 5 b-values.
-- Partition 1: 5 * (1+2) = 15 per group
-- Partition 2: 5 * (10+20) = 150 per group
-- Summing across partitions:
SELECT
    a,
    sum(finalizeAggregation(val)) AS total
FROM t_prefix_final FINAL BY a
GROUP BY a
ORDER BY a ASC
SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 7: Prefix FINAL BY after OPTIMIZE (single merged part)
-- ============================================================
SELECT '--- Part 7: prefix after OPTIMIZE ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

INSERT INTO t_prefix_final
    SELECT intDiv(number, 5), number % 5, sumState(toUInt64(1)) FROM numbers(25) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT intDiv(number, 5), number % 5, sumState(toUInt64(2)) FROM numbers(25) GROUP BY number;

OPTIMIZE TABLE t_prefix_final FINAL;

-- After OPTIMIZE, single merged part. FINAL BY a still collapses b.
-- 5 groups (a=0..4), each 5*3 = 15.
SELECT
    a,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY a
ORDER BY a ASC;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 8: Single-column prefix of multi-column key (identity)
-- ORDER BY (a, b, c), FINAL BY a
-- ============================================================
SELECT '--- Part 8: single-column prefix of 3-col key ---';

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    c UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b, c);

-- 27 rows: a in {0,1,2}, b in {0,1,2}, c in {0,1,2}
INSERT INTO t_prefix_final
    SELECT
        intDiv(number, 9) AS a,
        intDiv(number % 9, 3) AS b,
        number % 3 AS c,
        sumState(toUInt64(1))
    FROM numbers(27) GROUP BY number;
INSERT INTO t_prefix_final
    SELECT
        intDiv(number, 9) AS a,
        intDiv(number % 9, 3) AS b,
        number % 3 AS c,
        sumState(toUInt64(2))
    FROM numbers(27) GROUP BY number;

-- FINAL BY a: collapse b and c. 3 groups, each 9 rows * (1+2) = 27.
SELECT
    a,
    finalizeAggregation(val) AS total
FROM t_prefix_final FINAL BY a
ORDER BY a ASC;

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 9: Error — prefix FINAL BY on wrong column
-- ORDER BY (a, b), FINAL BY b → b is column 2, not column 1
-- ============================================================

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_prefix_final FINAL BY b; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_prefix_final;

-- ============================================================
-- Part 10: Error — prefix FINAL BY with direction reversal
-- ORDER BY (a, b, c), FINAL BY negate(a)
-- ============================================================

DROP TABLE IF EXISTS t_prefix_final;

CREATE TABLE t_prefix_final
(
    a UInt64,
    b UInt64,
    c UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b, c);

SELECT * FROM t_prefix_final FINAL BY negate(a); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_prefix_final;
