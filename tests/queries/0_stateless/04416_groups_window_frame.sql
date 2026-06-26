-- Tests for the GROUPS window frame (SQL:2011). Expected values match PostgreSQL / DuckDB.
-- A peer group is the set of rows equal on the ORDER BY key; GROUPS offsets count whole groups.

DROP TABLE IF EXISTS t_groups;
CREATE TABLE t_groups (ts Int32, x Int32) ENGINE = MergeTree ORDER BY (ts, x);
-- Peer groups by ts: {10: 1,2}, {20: 3}, {30: 4,5,6}
INSERT INTO t_groups VALUES (10, 1) (10, 2) (20, 3) (30, 4) (30, 5) (30, 6);

SELECT '-- CURRENT ROW AND 1 FOLLOWING';
SELECT ts, x,
       sum(x) OVER (ORDER BY ts GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s,
       arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- 1 PRECEDING AND 1 FOLLOWING';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- 2 PRECEDING AND 1 PRECEDING (frame behind current; empty for first group)';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN 2 PRECEDING AND 1 PRECEDING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- 1 FOLLOWING AND 2 FOLLOWING (frame ahead of current; empty at tail)';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- UNBOUNDED PRECEDING AND CURRENT ROW';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- CURRENT ROW AND UNBOUNDED FOLLOWING';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- 0 PRECEDING AND 0 FOLLOWING (= current peer group)';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN 0 PRECEDING AND 0 FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x;

SELECT '-- DESC ordering, 1 PRECEDING AND 1 FOLLOWING';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts DESC GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS g
FROM t_groups ORDER BY ts DESC, x;

SELECT '-- no ORDER BY (all rows are one peer group)';
SELECT x, arraySort(groupArray(x) OVER (GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS g
FROM t_groups ORDER BY x;

SELECT '-- cross-block invariance: customer case must be identical with max_block_size = 1';
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS g
FROM t_groups ORDER BY ts, x
SETTINGS max_block_size = 1;

SELECT '-- multi-column ORDER BY, 1 PRECEDING AND 1 FOLLOWING';
DROP TABLE IF EXISTS t_multi;
CREATE TABLE t_multi (a Int32, b Int32, x Int32) ENGINE = MergeTree ORDER BY (a, b, x);
-- Peer groups by (a, b): {(1,1): 10,11}, {(1,2): 12}, {(2,1): 13}
INSERT INTO t_multi VALUES (1, 1, 10) (1, 1, 11) (1, 2, 12) (2, 1, 13);
SELECT a, b, x, arraySort(groupArray(x) OVER (ORDER BY a, b GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS g
FROM t_multi ORDER BY a, b, x;

SELECT '-- cross-partition isolation: groups must not merge across partitions sharing ts';
DROP TABLE IF EXISTS t_part;
CREATE TABLE t_part (sym String, ts Int32, x Int32) ENGINE = MergeTree ORDER BY (sym, ts, x);
INSERT INTO t_part VALUES ('A', 10, 1) ('A', 10, 2) ('A', 20, 3) ('B', 10, 4) ('B', 10, 5) ('B', 20, 6);
SELECT sym, ts, x,
       arraySort(groupArray(x) OVER (PARTITION BY sym ORDER BY ts GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS g
FROM t_part ORDER BY sym, ts, x;

SELECT '-- Nullable ORDER BY column: NULLs are peers; offset path';
DROP TABLE IF EXISTS t_null;
CREATE TABLE t_null (ts Nullable(Int32), x Int32) ENGINE = MergeTree ORDER BY (ts, x) SETTINGS allow_nullable_key = 1;
INSERT INTO t_null VALUES (NULL, 1) (NULL, 2) (10, 3) (10, 4) (20, 5);
SELECT ts, x, arraySort(groupArray(x) OVER (ORDER BY ts GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)) AS g
FROM t_null ORDER BY ts, x;

SELECT '-- large peer groups exercise the galloping/binary equal-range search in the offset path';
-- Two peer groups of 5000 rows each. CURRENT ROW AND 1 FOLLOWING gives group A rows the sum over
-- A+B and group B rows the sum over B, so exactly two distinct sums. The result must not depend on
-- the block size (cross-block group stitching), so check several.
SELECT countDistinct(s) AS distinct_sums, min(s) AS lo, max(s) AS hi
FROM
(
    SELECT sum(number) OVER (ORDER BY (number >= 5000) GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s
    FROM numbers(10000)
)
SETTINGS max_block_size = 10000;
SELECT countDistinct(s) AS distinct_sums, min(s) AS lo, max(s) AS hi
FROM
(
    SELECT sum(number) OVER (ORDER BY (number >= 5000) GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS s
    FROM numbers(10000)
)
SETTINGS max_block_size = 1000;

-- A FOLLOWING-offset GROUPS start keeps waiting for more input until it can locate its peer
-- group, producing the correct future and empty frames at one-row blocks and one-row partitions.
SELECT '-- FOLLOWING-offset start, UNBOUNDED FOLLOWING end, one-row blocks';
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)) AS frame
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;

SELECT '-- FOLLOWING-offset start and end, one-row blocks';
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)) AS frame
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;

SELECT '-- future-only frame, no ORDER BY, single row (empty frame)';
SELECT number, arraySort(groupArray(number) OVER (GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) AS frame
FROM numbers(1) ORDER BY number;

SELECT '-- future-only frame, ORDER BY, single row (empty frame)';
SELECT number, arraySort(groupArray(number) OVER (ORDER BY number GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) AS frame
FROM numbers(1) ORDER BY number;

SELECT '-- offset PRECEDING/FOLLOWING frame over a numbers() input';
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM numbers(6) ORDER BY number;

SELECT '-- frame-respecting window functions over a GROUPS frame (read frame_start/frame_end)';
SELECT number,
       nth_value(number, 1) OVER (ORDER BY number GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS nth1,
       lagInFrame(number, 1, 999) OVER (ORDER BY number GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) AS lag1
FROM numbers(4) ORDER BY number;

SELECT '-- equivalent offset spellings and mixed UNBOUNDED/offset frames, one-row blocks';
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 0 FOLLOWING AND 0 FOLLOWING)) AS frame
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)) AS frame
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING)) AS frame
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;

SELECT '-- LowCardinality key, one-row blocks';
SELECT k, id, arraySort(groupArray(id) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT number AS id, toLowCardinality(['a', 'a', 'b', 'b', 'c', 'c'][number + 1]) AS k FROM numbers(6))
ORDER BY k, id SETTINGS max_block_size = 1;

SELECT '-- Nullable key with explicit NULLS LAST, one-row blocks';
SELECT k, id, arraySort(groupArray(id) OVER (ORDER BY k NULLS LAST GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT number AS id, CAST(if(number IN (1, 4), NULL, number % 3), 'Nullable(Int32)') AS k FROM numbers(6))
ORDER BY k NULLS LAST, id SETTINGS max_block_size = 1;

SELECT '-- multiple GROUPS frames in one query over an inherited named window';
SELECT number,
       arraySort(groupArray(number) OVER w1) AS current_group,
       arraySort(groupArray(number) OVER w2) AS neighbors
FROM numbers(6)
WINDOW
    base AS (ORDER BY intDiv(number, 2)),
    w1 AS (base GROUPS BETWEEN CURRENT ROW AND CURRENT ROW),
    w2 AS (base GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY number SETTINGS max_block_size = 1;

SELECT '-- aggregate-function-state output over a GROUPS frame';
SELECT number, finalizeAggregation(sumState(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS s
FROM numbers(6) ORDER BY number SETTINGS max_block_size = 1;

SELECT '-- single Tuple / Array ORDER BY key (generic equal-range interface)';
SELECT id, arraySort(groupArray(id) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT number AS id, (intDiv(number, 2), number % 2) AS k FROM numbers(6)) ORDER BY id SETTINGS max_block_size = 1;
SELECT id, arraySort(groupArray(id) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)) AS frame
FROM (SELECT number AS id, [intDiv(number, 2)] AS k FROM numbers(6)) ORDER BY id SETTINGS max_block_size = 1;

SELECT '-- partition boundary on a block edge: group walk reaching the partition end at one-row blocks';
DROP TABLE IF EXISTS t_edge;
CREATE TABLE t_edge (sym String, g Int32, x Int32) ENGINE = MergeTree ORDER BY (sym, g, x);
-- Two partitions, each with several groups. At max_block_size = 1 every partition boundary lands on
-- a block edge, so a FOLLOWING group walk reaches a partition end that is a real row (not blocksEnd).
INSERT INTO t_edge VALUES ('A', 1, 10) ('A', 1, 11) ('A', 2, 12) ('A', 3, 13) ('B', 1, 20) ('B', 1, 21) ('B', 2, 22);
SELECT sym, g, x, arraySort(groupArray(x) OVER (PARTITION BY sym ORDER BY g GROUPS BETWEEN 2 PRECEDING AND 1 FOLLOWING)) AS frame
FROM t_edge ORDER BY sym, g, x SETTINGS max_block_size = 1;
-- A larger partitioned input through the same path, checked for block-size invariance.
SELECT countDistinct(s) AS distinct_per_blocksize
FROM
(
    SELECT groupArray(s) AS s FROM (
        SELECT sum(x) OVER (PARTITION BY p ORDER BY g GROUPS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS s
        FROM (SELECT number % 4 AS p, intDiv(number, 3) % 6 AS g, number AS x FROM numbers(300))
        ORDER BY p, g, x
    ) SETTINGS max_block_size = 1
    UNION ALL
    SELECT groupArray(s) AS s FROM (
        SELECT sum(x) OVER (PARTITION BY p ORDER BY g GROUPS BETWEEN 2 PRECEDING AND 1 FOLLOWING) AS s
        FROM (SELECT number % 4 AS p, intDiv(number, 3) % 6 AS g, number AS x FROM numbers(300))
        ORDER BY p, g, x
    ) SETTINGS max_block_size = 65536
);

SELECT '-- block-edge resolution in findPeerGroupEnd: partition boundary vs more-input, multi-row blocks';
-- max_block_size = 2 places the partition boundary and the group boundaries exactly on block edges,
-- exercising the break-and-resolve path (real partition boundary vs waiting for the next block).
WITH base AS (SELECT * FROM values('p UInt8, k UInt8, x UInt8', (0, 1, 1), (0, 1, 2), (1, 1, 3), (1, 1, 4), (1, 2, 5), (1, 2, 6)))
SELECT p, k, x, arraySort(groupArray(x) OVER (PARTITION BY p ORDER BY k GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING)) AS frame
FROM base ORDER BY p, k, x SETTINGS max_block_size = 2;
-- Each peer group is one block; a single future group is resolved across the block edge.
SELECT number, arraySort(groupArray(number) OVER (ORDER BY intDiv(number, 2) GROUPS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)) AS frame
FROM numbers(8) ORDER BY number SETTINGS max_block_size = 2;

SELECT '-- every window function documented with a GROUPS frame accepts one';
-- One row per dedicated window function, each over a GROUPS frame, confirming the syntax the
-- per-function docs advertise (`ROWS, RANGE, or GROUPS`) actually works.
SELECT
    toInt64(number) AS n,
    row_number()              OVER w AS rn,
    rank()                    OVER w AS rnk,
    dense_rank()              OVER w AS drnk,
    first_value(n)            OVER w AS fv,
    last_value(n)             OVER w AS lv,
    nth_value(n, 2)           OVER w AS nv,
    lagInFrame(n, 1, -1)      OVER w AS lagf,
    leadInFrame(n, 1, -1)     OVER w AS leadf
FROM numbers(5)
WINDOW w AS (ORDER BY n GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
ORDER BY n;
SELECT number, nonNegativeDerivative(number * 10, toDateTime(number)) OVER (ORDER BY number GROUPS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS d
FROM numbers(5) ORDER BY number;

DROP TABLE IF EXISTS t_groups;
DROP TABLE IF EXISTS t_multi;
DROP TABLE IF EXISTS t_part;
DROP TABLE IF EXISTS t_null;
DROP TABLE IF EXISTS t_edge;
