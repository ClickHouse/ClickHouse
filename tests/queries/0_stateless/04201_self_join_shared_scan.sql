SET enable_analyzer = 1;
SET query_plan_optimize_self_join_shared_scan = 1;
SET enable_join_runtime_filters = 0;
SET query_plan_filter_push_down = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;

DROP TABLE IF EXISTS t_sjss;
CREATE TABLE t_sjss (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss SELECT number, toString(number) FROM numbers(10);

-- Correctness with optimization on.
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x;

-- Same query without optimization, results must match.
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x SETTINGS query_plan_optimize_self_join_shared_scan = 0;

-- Plan shape: single scan, save buffer, read buffer.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
);

-- Negative: FINAL must NOT trigger optimization (still 2 ReadFromMergeTree).
DROP TABLE IF EXISTS t_sjss_rmt;
CREATE TABLE t_sjss_rmt (x UInt64, y String) ENGINE = ReplacingMergeTree ORDER BY x;
INSERT INTO t_sjss_rmt SELECT number, toString(number) FROM numbers(10);

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss_rmt AS a FINAL INNER JOIN t_sjss_rmt AS b ON a.x = b.x
);

-- Negative: Different tables must NOT trigger optimization.
DROP TABLE IF EXISTS t_sjss2;
CREATE TABLE t_sjss2 (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss2 SELECT number, toString(number) FROM numbers(10);

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss2 AS b ON a.x = b.x
);

-- LEFT JOIN should also fire.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a LEFT JOIN t_sjss AS b ON a.x = b.x
);

-- Non-hash algorithm (full_sorting_merge) must NOT trigger optimization,
-- and must not error out with "Can't execute any of specified join algorithms".
SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x ORDER BY a.x
SETTINGS join_algorithm = 'full_sorting_merge';

SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS join_algorithm = 'full_sorting_merge'
);

-- Setting off must keep two scans.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS query_plan_optimize_self_join_shared_scan = 0
);

-- Negative: non-shared snapshot must NOT trigger optimization (2 scans, no buffer).
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.y FROM t_sjss AS a INNER JOIN t_sjss AS b ON a.x = b.x
    SETTINGS enable_shared_storage_snapshot_in_query = 0
);

DROP TABLE t_sjss;
DROP TABLE t_sjss2;
DROP TABLE t_sjss_rmt;
