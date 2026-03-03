-- Tags: no-random-settings

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

DROP TABLE t_topn;
DROP TABLE t_topn_small;
DROP TABLE t_topn_unsorted;
