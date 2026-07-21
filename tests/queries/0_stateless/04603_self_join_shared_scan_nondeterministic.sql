-- Functions that are not deterministic within a query (`rowNumberInAllBlocks`, `blockNumber`, ...)
-- depend on the runtime stream, so an `ExpressionStep` computing them must block the rewrite.

SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot
SET query_plan_join_swap_table = 0; -- keep the expression under test on the probe side
SET query_plan_optimize_join_order_randomize = 0; -- join order randomization may swap the sides

DROP TABLE IF EXISTS t_sjss_nondet;
CREATE TABLE t_sjss_nondet (x UInt64, y String) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_nondet SELECT number, toString(number) FROM numbers(100);

-- `rowNumberInAllBlocks` on the probe side: two scans, no buffer.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.rn, b.y
    FROM (SELECT rowNumberInAllBlocks() AS rn, x FROM t_sjss_nondet) AS a
    INNER JOIN t_sjss_nondet AS b ON a.x = b.x
);

-- `blockNumber`.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.bn, b.y
    FROM (SELECT blockNumber() AS bn, x FROM t_sjss_nondet) AS a
    INNER JOIN t_sjss_nondet AS b ON a.x = b.x
);

-- `rand`.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.r, b.y
    FROM (SELECT rand() AS r, x FROM t_sjss_nondet) AS a
    INNER JOIN t_sjss_nondet AS b ON a.x = b.x
);

-- A deterministic probe-side expression still allows the rewrite.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x2, b.y
    FROM (SELECT x + 1 AS x2, x FROM t_sjss_nondet) AS a
    INNER JOIN t_sjss_nondet AS b ON a.x = b.x
);

-- Each probe row keeps its own scan's numbering.
SELECT count(), uniqExact(rn), min(rn), max(rn)
FROM (
    SELECT a.rn AS rn
    FROM (SELECT rowNumberInAllBlocks() AS rn, x FROM t_sjss_nondet) AS a
    INNER JOIN t_sjss_nondet AS b ON a.x = b.x
)
SETTINGS max_threads = 1;

DROP TABLE t_sjss_nondet;
