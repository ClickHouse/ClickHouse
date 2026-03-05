-- Tags: no-random-settings, no-fasttest

DROP TABLE IF EXISTS t_topn;

CREATE TABLE t_topn (trace_id String, start_time DateTime, service_name String, value UInt64)
ENGINE = MergeTree ORDER BY start_time;

INSERT INTO t_topn SELECT
    'trace_' || toString(number % 1000),
    toDateTime('2024-01-01') + number,
    'service_' || toString(number % 5),
    number
FROM numbers(10000);

-- Correctness: compare optimized vs unoptimized results for max + DESC

SELECT '-- max DESC: optimized';
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- max DESC: unoptimized';
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Correctness: min + ASC

SELECT '-- min ASC: optimized';
SELECT trace_id, min(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- min ASC: unoptimized';
SELECT trace_id, min(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Correctness: max with any

SELECT '-- max + any: optimized';
SELECT trace_id, max(start_time) AS m, any(service_name) AS s
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

-- EXPLAIN PLAN: verify TopNAggregating step appears

SELECT '-- EXPLAIN PLAN';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Negative case: incompatible aggregate (count) - optimization should NOT apply

SELECT '-- EXPLAIN with count (should NOT optimize)';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m, count(*) AS c
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Negative case: WITH TIES - should NOT apply

SELECT '-- EXPLAIN with WITH TIES (should NOT optimize)';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5 WITH TIES
SETTINGS optimize_topn_aggregation = 1;

-- Negative case: wrong sort direction (max + ASC) - should NOT apply

SELECT '-- EXPLAIN max ASC (should NOT optimize)';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Edge case: K > total groups (small table, 10 groups)
DROP TABLE IF EXISTS t_topn_small;
CREATE TABLE t_topn_small (trace_id String, start_time DateTime)
ENGINE = MergeTree ORDER BY start_time;
INSERT INTO t_topn_small SELECT 'trace_' || toString(number % 10), toDateTime('2024-01-01') + number FROM numbers(100);

SELECT '-- K larger than groups';
SELECT trace_id, max(start_time) AS m
FROM t_topn_small
GROUP BY trace_id
ORDER BY m DESC
LIMIT 100
SETTINGS optimize_topn_aggregation = 1;

-- Empty result
SELECT '-- empty table';
SELECT trace_id, max(start_time) AS m
FROM t_topn
WHERE 0
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- Mode 2: table not sorted by aggregate argument
DROP TABLE IF EXISTS t_topn_unsorted;

CREATE TABLE t_topn_unsorted (trace_id String, start_time DateTime, value UInt64)
ENGINE = MergeTree ORDER BY trace_id;

INSERT INTO t_topn_unsorted SELECT
    'trace_' || toString(number % 100),
    toDateTime('2024-01-01') + number,
    number
FROM numbers(1000);

SELECT '-- Mode 2 (unsorted): max DESC';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- Mode 2 (unsorted): reference';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Negative case: OFFSET should NOT optimize (correctness)

SELECT '-- EXPLAIN LIMIT OFFSET (should NOT optimize)';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5 OFFSET 10
SETTINGS optimize_topn_aggregation = 1;

-- Nullable column with NULLS FIRST/LAST

DROP TABLE IF EXISTS t_topn_nullable;
CREATE TABLE t_topn_nullable (key String, val Nullable(UInt64))
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_topn_nullable VALUES ('a', 100), ('b', NULL), ('c', 50), ('d', 200), ('e', 10), ('f', 150);

SELECT '-- NULLS LAST: optimized';
SELECT key, max(val) AS m
FROM t_topn_nullable
GROUP BY key
ORDER BY m DESC NULLS LAST
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- NULLS LAST: reference';
SELECT key, max(val) AS m
FROM t_topn_nullable
GROUP BY key
ORDER BY m DESC NULLS LAST
LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

SELECT '-- NULLS FIRST: optimized';
SELECT key, max(val) AS m
FROM t_topn_nullable
GROUP BY key
ORDER BY m DESC NULLS FIRST
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- NULLS FIRST: reference';
SELECT key, max(val) AS m
FROM t_topn_nullable
GROUP BY key
ORDER BY m DESC NULLS FIRST
LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

-- Collation-sensitive ordering test (Mode 2, unsorted by aggregate arg)
DROP TABLE IF EXISTS t_topn_collation;
CREATE TABLE t_topn_collation (key UInt32, val String)
ENGINE = MergeTree ORDER BY key;

INSERT INTO t_topn_collation VALUES (1, 'ä'), (2, 'z'), (3, 'a'), (4, 'ö'), (5, 'b');

SELECT '-- collation: optimized';
SELECT key, min(val) AS m
FROM t_topn_collation
GROUP BY key
ORDER BY m ASC COLLATE 'en'
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- collation: reference';
SELECT key, min(val) AS m
FROM t_topn_collation
GROUP BY key
ORDER BY m ASC COLLATE 'en'
LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

-- Mode 1 early termination: verify TopNAggregating is used and reads in reverse order
SELECT '-- mode 1 EXPLAIN sorted input';
EXPLAIN PLAN
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

-- Mode 1: correctness with small limit
SELECT '-- mode 1 small limit: optimized';
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 3
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- mode 1 small limit: reference';
SELECT trace_id, max(start_time) AS m
FROM t_topn
GROUP BY trace_id
ORDER BY m DESC
LIMIT 3
SETTINGS optimize_topn_aggregation = 0;

-- argMin / argMax tests
DROP TABLE IF EXISTS t_topn_argminmax;
CREATE TABLE t_topn_argminmax (grp String, ts DateTime, payload String)
ENGINE = MergeTree ORDER BY ts;

INSERT INTO t_topn_argminmax SELECT
    'g' || toString(number % 50),
    toDateTime('2024-01-01') + number,
    'payload_' || toString(number)
FROM numbers(500);

SELECT '-- argMin ASC: optimized';
SELECT grp, argMin(payload, ts) AS earliest
FROM t_topn_argminmax
GROUP BY grp
ORDER BY earliest ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- argMin ASC: reference';
SELECT grp, argMin(payload, ts) AS earliest
FROM t_topn_argminmax
GROUP BY grp
ORDER BY earliest ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

SELECT '-- argMax DESC: optimized';
SELECT grp, argMax(payload, ts) AS latest
FROM t_topn_argminmax
GROUP BY grp
ORDER BY latest DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- argMax DESC: reference';
SELECT grp, argMax(payload, ts) AS latest
FROM t_topn_argminmax
GROUP BY grp
ORDER BY latest DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Tie-heavy dataset: many rows per group, same aggregate value for many groups
DROP TABLE IF EXISTS t_topn_ties;
CREATE TABLE t_topn_ties (grp String, val UInt64)
ENGINE = MergeTree ORDER BY val;

-- 100 groups, 10 rows each; max(val) for each group = grp_num * 100 + 9
INSERT INTO t_topn_ties SELECT
    'group_' || leftPad(toString(number % 100), 3, '0'),
    (number % 100) * 100 + (number / 100)
FROM numbers(1000);

SELECT '-- ties: optimized';
SELECT grp, max(val) AS m
FROM t_topn_ties
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- ties: reference';
SELECT grp, max(val) AS m
FROM t_topn_ties
GROUP BY grp
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Multiple aggregates: max + argMax together
SELECT '-- multi-agg max+argMax: optimized';
SELECT grp, max(ts) AS latest_ts, argMax(payload, ts) AS latest_payload
FROM t_topn_argminmax
GROUP BY grp
ORDER BY latest_ts DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- multi-agg max+argMax: reference';
SELECT grp, max(ts) AS latest_ts, argMax(payload, ts) AS latest_payload
FROM t_topn_argminmax
GROUP BY grp
ORDER BY latest_ts DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- EXPLAIN: argMin falls through to standard pipeline because its output (payload)
-- has different sort order than the sort key (ts), so neither Mode 1 (early termination)
-- nor Mode 2 (threshold pruning) can safely apply.
SELECT '-- EXPLAIN argMin (standard pipeline)';
EXPLAIN PLAN
SELECT grp, argMin(payload, ts) AS earliest
FROM t_topn_argminmax
GROUP BY grp
ORDER BY earliest ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

-- ====== Threshold pruning (Mode 2) ======

-- Test threshold pruning with max DESC on unsorted table
SELECT '-- Mode 2 threshold pruning: max DESC';
SELECT group_id, max(val) AS m
FROM (SELECT number % 50 AS group_id, number AS val FROM numbers(5000))
GROUP BY group_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- Mode 2 threshold pruning reference';
SELECT group_id, max(val) AS m
FROM (SELECT number % 50 AS group_id, number AS val FROM numbers(5000))
GROUP BY group_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Test threshold pruning with min ASC
SELECT '-- Mode 2 threshold pruning: min ASC';
SELECT group_id, min(val) AS m
FROM (SELECT number % 50 AS group_id, number * 7 % 1000 AS val FROM numbers(5000))
GROUP BY group_id
ORDER BY m ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1;

SELECT '-- Mode 2 threshold pruning min ASC reference';
SELECT group_id, min(val) AS m
FROM (SELECT number % 50 AS group_id, number * 7 % 1000 AS val FROM numbers(5000))
GROUP BY group_id
ORDER BY m ASC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- Pruning level tests: verify correctness at each level on MergeTree table
-- Level 0: direct compute only (no threshold, no filter)
SELECT '-- Pruning level 0: max DESC';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 0;

-- Level 1: in-transform threshold pruning (no dynamic filter)
SELECT '-- Pruning level 1: max DESC';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 1;

-- Level 2: full (threshold + dynamic filter) -- requires use_top_k_dynamic_filtering
SELECT '-- Pruning level 2: max DESC';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 2, use_top_k_dynamic_filtering = 1;

-- Reference (unoptimized)
SELECT '-- Pruning level reference';
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 0;

-- EXPLAIN at each level: verify plan differences
SELECT '-- EXPLAIN pruning level 0';
EXPLAIN
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 0;

SELECT '-- EXPLAIN pruning level 1';
EXPLAIN
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, topn_aggregation_pruning_level = 1;

-- EXPLAIN: verify threshold pruning and __topKFilter prewhere on MergeTree table
-- requires use_top_k_dynamic_filtering = 1 since it defaults to off
SELECT '-- EXPLAIN threshold pruning with prewhere';
EXPLAIN actions=1
SELECT trace_id, max(start_time) AS m
FROM t_topn_unsorted
GROUP BY trace_id
ORDER BY m DESC
LIMIT 5
SETTINGS optimize_topn_aggregation = 1, use_top_k_dynamic_filtering = 1;

DROP TABLE t_topn;
DROP TABLE t_topn_small;
DROP TABLE t_topn_unsorted;
DROP TABLE t_topn_nullable;
DROP TABLE t_topn_collation;
DROP TABLE t_topn_argminmax;
DROP TABLE t_topn_ties;
