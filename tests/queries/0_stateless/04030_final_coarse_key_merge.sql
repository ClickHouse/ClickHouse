-- Test that FINAL BY can merge rows at coarser granularity. Each FINAL BY
-- expression must reference only the corresponding sorting key column and
-- be a deterministic, same-direction monotonic function. The number of
-- FINAL BY expressions must exactly match the number of sorting key columns.

SET enable_analyzer = 1;

-- ============================================================
-- Part 1: Basic coarse merge with AggregatingMergeTree
-- ORDER BY unix_time
-- FINAL BY intDiv(unix_time, 10)
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 5;

DROP TABLE t_final_by;

-- ============================================================
-- Part 2: Composite sorting key — prefix identity, last column coarsened
-- ORDER BY (token, pair, unix_time)
-- FINAL BY (token, pair, intDiv(unix_time, 10))
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    token String,
    pair String,
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (token, pair, unix_time);

INSERT INTO t_final_by
    SELECT 'BTC', 'USD', number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT 'BTC', 'USD', number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT 'ETH', 'USD', number, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY token, pair, intDiv(unix_time, 10)
PREWHERE token = 'BTC' AND pair = 'USD'
ORDER BY bucket ASC
LIMIT 5;

DROP TABLE t_final_by;

-- ============================================================
-- Part 3: SummingMergeTree
-- ORDER BY unix_time
-- FINAL BY intDiv(unix_time, 10)
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val UInt64
)
ENGINE = SummingMergeTree
ORDER BY unix_time;

INSERT INTO t_final_by SELECT number, 1 FROM numbers(100);
INSERT INTO t_final_by SELECT number, 2 FROM numbers(100);

SELECT
    intDiv(unix_time, 10) AS bucket,
    val AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 5;

DROP TABLE t_final_by;

-- ============================================================
-- Part 4: Identity FINAL BY — equivalent to plain FINAL
-- ORDER BY unix_time
-- FINAL BY unix_time
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(10) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(10) GROUP BY number;

SELECT
    unix_time,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY unix_time
ORDER BY unix_time ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 5: ORDER BY with expression — FINAL BY uses the same expression (identity)
-- ORDER BY intDiv(unix_time, 100)
-- FINAL BY intDiv(unix_time, 100)
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY intDiv(unix_time, 100);

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(200) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(200) GROUP BY number;

-- Identity: FINAL BY repeats the sorting key expression exactly.
SELECT
    intDiv(unix_time, 100) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 100)
ORDER BY bucket ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 6: ORDER BY with expression — FINAL BY is a monotonic function of it
-- ORDER BY toDate(ts)
-- FINAL BY toStartOfMonth(toDate(ts))   — monotonic over Date
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    ts DateTime,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY toDate(ts);

INSERT INTO t_final_by
    SELECT toDateTime('2024-01-01') + number * 3600, sumState(toUInt64(1))
    FROM numbers(1440) GROUP BY number;
INSERT INTO t_final_by
    SELECT toDateTime('2024-01-01') + number * 3600, sumState(toUInt64(2))
    FROM numbers(1440) GROUP BY number;

-- toStartOfMonth(toDate(ts)) is a monotonic function of toDate(ts).
SELECT
    toStartOfMonth(toDate(ts)) AS month,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY toStartOfMonth(toDate(ts))
ORDER BY month ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 7: Error — FINAL BY count fewer than sorting key columns
-- ORDER BY (a, b), FINAL BY a  → must specify all columns
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_final_by FINAL BY a; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 8: Error — FINAL BY count more than sorting key columns
-- ORDER BY (a, b), FINAL BY a, b, a
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_final_by FINAL BY a, b, a; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 9: Error — columns in wrong order
-- ORDER BY (a, b), FINAL BY b, a
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_final_by FINAL BY b, a; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 10: Error — direction reversal (negate)
-- ORDER BY (a, b), FINAL BY negate(a), b
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_final_by FINAL BY negate(a), b; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 11: Error — constant expression (not referencing sorting key)
-- ORDER BY a, FINAL BY 42
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY a;

SELECT * FROM t_final_by FINAL BY 42; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 12: Error — expression referencing multiple columns
-- ORDER BY (a, b), FINAL BY a + b, b
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    b UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY (a, b);

SELECT * FROM t_final_by FINAL BY a + b, b; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 13: Error — ORDER BY has expression, FINAL BY references raw column
-- ORDER BY intDiv(a, 100), FINAL BY a  → `a` is not sorting key output
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY intDiv(a, 100);

SELECT * FROM t_final_by FINAL BY a; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 14: Error — unsupported engine: ReplacingMergeTree
-- ============================================================

DROP TABLE IF EXISTS t_final_by;
CREATE TABLE t_final_by (a UInt64, val UInt64) ENGINE = ReplacingMergeTree ORDER BY a;
SELECT * FROM t_final_by FINAL BY a; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_final_by;

-- ============================================================
-- Part 15: Error — unsupported engine: CollapsingMergeTree
-- ============================================================

DROP TABLE IF EXISTS t_final_by;
CREATE TABLE t_final_by (a UInt64, val UInt64, sign Int8) ENGINE = CollapsingMergeTree(sign) ORDER BY a;
SELECT * FROM t_final_by FINAL BY a; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_final_by;

-- ============================================================
-- Part 16: Error — unsupported engine: VersionedCollapsingMergeTree
-- ============================================================

DROP TABLE IF EXISTS t_final_by;
CREATE TABLE t_final_by (a UInt64, val UInt64, sign Int8, ver UInt64) ENGINE = VersionedCollapsingMergeTree(sign, ver) ORDER BY a;
SELECT * FROM t_final_by FINAL BY a; -- { serverError BAD_ARGUMENTS }
DROP TABLE t_final_by;

-- ============================================================
-- Part 17: Error — unsupported engine: MergeTree (Ordinary)
-- ============================================================

DROP TABLE IF EXISTS t_final_by;
CREATE TABLE t_final_by (a UInt64, val UInt64) ENGINE = MergeTree ORDER BY a;
SELECT * FROM t_final_by FINAL BY a; -- { serverError ILLEGAL_FINAL }
DROP TABLE t_final_by;

-- ============================================================
-- Part 18: PREWHERE with coarsened FINAL BY — filter narrows rows before merge
-- ORDER BY unix_time
-- FINAL BY intDiv(unix_time, 10)
-- PREWHERE unix_time >= 20 AND unix_time < 40  → keeps buckets 2, 3
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
PREWHERE unix_time >= 20 AND unix_time < 40
ORDER BY bucket ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 19: WHERE on non-key column with coarsened FINAL BY
-- ORDER BY unix_time
-- FINAL BY intDiv(unix_time, 10)
-- WHERE with computed column filter
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    unix_time UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(50) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
WHERE intDiv(unix_time, 10) >= 3
ORDER BY bucket ASC;

DROP TABLE t_final_by;

----------------------------------------------------------------------
-- Part 20: SELECT alias in FINAL BY
--
-- Verifies that FINAL BY can reference a SELECT alias that wraps a
-- monotonic function of the sorting key column. The alias `month` is
-- defined as `toStartOfMonth(toDate(ts))` in the SELECT list, and
-- FINAL BY uses `month` directly.
----------------------------------------------------------------------
SELECT '--- Part 20: SELECT alias in FINAL BY ---';

CREATE TABLE t_final_by (ts DateTime, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY ts;

INSERT INTO t_final_by
    SELECT
        toDateTime('2024-01-15 10:00:00') + number * 86400 AS ts,
        sumState(toUInt64(1))
    FROM numbers(60)
    GROUP BY ts;

-- Use SELECT alias `month` in FINAL BY
SELECT
    toStartOfMonth(toDate(ts)) AS month,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY month
ORDER BY month ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 21: SummingMergeTree with composite sorting key
-- ORDER BY (region, unix_time)
-- FINAL BY (region, intDiv(unix_time, 10))
-- ============================================================
SELECT '--- Part 21: SummingMergeTree composite key ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    region String,
    unix_time UInt64,
    val UInt64
)
ENGINE = SummingMergeTree
ORDER BY (region, unix_time);

INSERT INTO t_final_by SELECT 'us', number, 1 FROM numbers(30);
INSERT INTO t_final_by SELECT 'us', number, 2 FROM numbers(30);
INSERT INTO t_final_by SELECT 'eu', number, 1 FROM numbers(20);

SELECT
    region,
    intDiv(unix_time, 10) AS bucket,
    val AS total
FROM t_final_by FINAL BY region, intDiv(unix_time, 10)
ORDER BY region ASC, bucket ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 22: Empty table — FINAL BY is validated even with no data
-- ============================================================
SELECT '--- Part 22: Empty table validation ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (a UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY a;

-- Valid FINAL BY on empty table should return empty result
SELECT count() FROM t_final_by FINAL BY a;

-- Invalid FINAL BY on empty table should still error
SELECT * FROM t_final_by FINAL BY a + 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;

-- ============================================================
-- Part 23: FINAL BY with LIMIT
-- ============================================================
SELECT '--- Part 23: FINAL BY with LIMIT ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 3;

DROP TABLE t_final_by;

-- ============================================================
-- Part 24: FINAL BY in subquery
-- ============================================================
SELECT '--- Part 24: FINAL BY in subquery ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(50) GROUP BY number;

SELECT bucket, total FROM (
    SELECT
        intDiv(unix_time, 10) AS bucket,
        finalizeAggregation(val) AS total
    FROM t_final_by FINAL BY intDiv(unix_time, 10)
)
ORDER BY bucket ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 25: FINAL BY with do_not_merge_across_partitions_select_final
-- ============================================================
SELECT '--- Part 25: FINAL BY with do_not_merge_across_partitions ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (part_key UInt64, unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree PARTITION BY part_key ORDER BY unix_time;

-- Two partitions, each with two parts
INSERT INTO t_final_by
    SELECT 1, number, sumState(toUInt64(1)) FROM numbers(30) GROUP BY number;
INSERT INTO t_final_by
    SELECT 1, number, sumState(toUInt64(2)) FROM numbers(30) GROUP BY number;
INSERT INTO t_final_by
    SELECT 2, number, sumState(toUInt64(10)) FROM numbers(30) GROUP BY number;
INSERT INTO t_final_by
    SELECT 2, number, sumState(toUInt64(20)) FROM numbers(30) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    sum(finalizeAggregation(val)) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
GROUP BY bucket
ORDER BY bucket ASC
SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_final_by;

-- ============================================================
-- Part 26: FINAL BY with many parts in same partition (3 inserts)
-- ============================================================
SELECT '--- Part 26: FINAL BY with many parts ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(50) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(4)) FROM numbers(50) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    sum(finalizeAggregation(val)) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
GROUP BY bucket
ORDER BY bucket ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 27: FINAL BY with optimize_read_in_order = 0
-- Tests the code path where read_in_order is disabled.
-- ============================================================
SELECT '--- Part 27: optimize_read_in_order = 0 ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(50) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(50) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
SETTINGS optimize_read_in_order = 0;

DROP TABLE t_final_by;

-- ============================================================
-- Part 28: FINAL BY with multiple final threads
-- Tests multi-threaded merge pipeline.
-- ============================================================
SELECT '--- Part 28: multi-threaded FINAL BY ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(100) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(100) GROUP BY number;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
LIMIT 5
SETTINGS max_final_threads = 4;

DROP TABLE t_final_by;

-- ============================================================
-- Part 29: FINAL BY after OPTIMIZE FINAL (single merged part with level > 0)
-- When do_not_merge_across_partitions_select_final is set, single parts
-- with level > 0 normally skip the merge step. FINAL BY must still
-- trigger re-aggregation because the coarsened key differs from the
-- stored fine-grained key.
-- ============================================================
SELECT '--- Part 29: FINAL BY after OPTIMIZE ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(30) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(30) GROUP BY number;

-- Merge all parts into one with level > 0
OPTIMIZE TABLE t_final_by FINAL;

SELECT
    intDiv(unix_time, 10) AS bucket,
    finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY bucket ASC
SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_final_by;

-- ============================================================
-- Part 30: FINAL BY columns not leaked into output
-- Verifies that SELECT with specific columns does not include
-- intermediate FINAL BY expression columns.
-- ============================================================
SELECT '--- Part 30: output columns ---';

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by (unix_time UInt64, val AggregateFunction(sum, UInt64))
ENGINE = AggregatingMergeTree ORDER BY unix_time;

INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(1)) FROM numbers(20) GROUP BY number;
INSERT INTO t_final_by
    SELECT number, sumState(toUInt64(2)) FROM numbers(20) GROUP BY number;

-- Only request `val`; the FINAL BY expression should not appear in output.
SELECT finalizeAggregation(val) AS total
FROM t_final_by FINAL BY intDiv(unix_time, 10)
ORDER BY total ASC;

DROP TABLE t_final_by;

-- ============================================================
-- Part 31: Error — non-monotonic function (xxHash64)
-- ORDER BY a, FINAL BY xxHash64(a)
-- ============================================================

DROP TABLE IF EXISTS t_final_by;

CREATE TABLE t_final_by
(
    a UInt64,
    val AggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
ORDER BY a;

SELECT * FROM t_final_by FINAL BY xxHash64(a); -- { serverError BAD_ARGUMENTS }

DROP TABLE t_final_by;
