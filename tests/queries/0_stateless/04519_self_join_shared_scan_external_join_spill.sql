SET enable_analyzer = 1; -- the rewrite requires the analyzer
SET query_plan_optimize_self_join_shared_scan = 1; -- the setting under test
SET enable_join_runtime_filters = 0; -- a runtime filter makes the scan non-plain and blocks the rewrite
SET enable_parallel_replicas = 0; -- reading with parallel replicas blocks the rewrite
SET enable_shared_storage_snapshot_in_query = 1; -- the rewrite requires both scans to share one storage snapshot
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS t_sjss_spill;
CREATE TABLE t_sjss_spill (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_sjss_spill SELECT number FROM numbers(10);

-- A non-zero external-join threshold makes the hash family run as `SpillingHashJoin`, which does
-- not override `pipelineType` and so is still `FillRightFirst`: the build side is drained
-- completely before the probe side is read. That is the only ordering property the rewrite needs,
-- so spilling must not block it (1 scan plus a buffer in every case below).

-- Absolute threshold set.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.x FROM t_sjss_spill AS a INNER JOIN t_sjss_spill AS b ON a.x = b.x
    SETTINGS max_bytes_before_external_join = 1000000000, max_bytes_ratio_before_external_join = 0
);

-- Ratio threshold set (the default configuration).
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.x FROM t_sjss_spill AS a INNER JOIN t_sjss_spill AS b ON a.x = b.x
    SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0.5
);

-- Spilling fully disabled.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%SaveSubqueryResultToBuffer%') AS save_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count
FROM (
    EXPLAIN actions = 0
    SELECT a.x, b.x FROM t_sjss_spill AS a INNER JOIN t_sjss_spill AS b ON a.x = b.x
    SETTINGS max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0
);

-- A threshold low enough that the build side actually spills, so the rewrite is exercised against
-- a join that really goes to disk rather than one that merely could.
SELECT a.x, b.x FROM t_sjss_spill AS a INNER JOIN t_sjss_spill AS b ON a.x = b.x
ORDER BY a.x
SETTINGS max_bytes_before_external_join = 1, max_bytes_ratio_before_external_join = 0;

DROP TABLE t_sjss_spill;
