-- Parallel replicas support for Merge tables and the merge() table function.
-- https://github.com/ClickHouse/ClickHouse/issues/67770

SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_pr_merge_1;
DROP TABLE IF EXISTS t_pr_merge_2;
DROP TABLE IF EXISTS t_pr_merge_log;
DROP TABLE IF EXISTS t_pr_merge;
DROP TABLE IF EXISTS t_pr_merge_over_log;
DROP TABLE IF EXISTS t_pr_merge_over_nothing;

CREATE TABLE t_pr_merge_1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
CREATE TABLE t_pr_merge_2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 10;
INSERT INTO t_pr_merge_1 SELECT number, number * 2 FROM numbers(10000);
INSERT INTO t_pr_merge_2 SELECT number + 10000, number FROM numbers(10000);

CREATE TABLE t_pr_merge ENGINE = Merge(currentDatabase(), '^t_pr_merge_[12]$');

SELECT '-- non-parallel';
SELECT count(), sum(k), sum(v) FROM t_pr_merge;

SET enable_analyzer = 1;
SET max_threads = 4;
SET enable_parallel_replicas = 1, max_parallel_replicas = 3, cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET parallel_replicas_local_plan = 1;
SET automatic_parallel_replicas_mode = 0;

-- Setting disabled: no distribution, plain Aggregating
SELECT '-- parallel_replicas_allow_merge_tables = 0';
SELECT trimLeft(explain) AS e FROM (EXPLAIN SELECT count(), sum(k), sum(v) FROM t_pr_merge SETTINGS parallel_replicas_allow_merge_tables = 0) WHERE e IN ('Aggregating', 'MergingAggregated');
SELECT count(), sum(k), sum(v) FROM t_pr_merge SETTINGS parallel_replicas_allow_merge_tables = 0;

SET parallel_replicas_allow_merge_tables = 1;

-- Setting enabled: aggregation is distributed, the initiator merges partial aggregation states
SELECT '-- parallel_replicas_allow_merge_tables = 1';
SELECT trimLeft(explain) AS e FROM (EXPLAIN SELECT count(), sum(k), sum(v) FROM t_pr_merge) WHERE e IN ('Aggregating', 'MergingAggregated');

-- Slow the initiator's local reads so that remote replicas actually produce rows;
-- rows read both locally and remotely would then surface as wrong aggregates.
SYSTEM ENABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

SELECT count(), sum(k), sum(v) FROM t_pr_merge;

SELECT '-- merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pr_merge_[12]$');

SELECT '-- filter on the _table virtual column';
SELECT count(), sum(k), sum(v) FROM t_pr_merge WHERE _table = 't_pr_merge_2';

SELECT '-- plain SELECT without aggregation';
SELECT k, v FROM t_pr_merge WHERE k IN (1, 9999, 10000, 19999) ORDER BY k;

-- Ordered query with a limit: read-in-order optimization materializes the child plans of the
-- Merge table; it must not clash with enabling parallel replicas reading on the same step.
SELECT '-- ORDER BY with LIMIT';
SELECT k, v FROM t_pr_merge ORDER BY k LIMIT 5;

SYSTEM DISABLE FAILPOINT slowdown_parallel_replicas_local_plan_read;

-- FINAL is not supported with parallel replicas: the query must fall back to single-replica
-- execution instead of failing, both for the Merge table and the merge() table function.
SELECT '-- FINAL over a Merge table';
SELECT count(), sum(k), sum(v) FROM t_pr_merge FINAL;
SELECT '-- FINAL over the merge() table function';
SELECT count(), sum(k), sum(v) FROM merge(currentDatabase(), '^t_pr_merge_[12]$') FINAL;

-- A Merge over a non-MergeTree table cannot coordinate reading, parallel replicas must not be used
SELECT '-- Merge over a non-MergeTree table';
CREATE TABLE t_pr_merge_log (k UInt64, v UInt64) ENGINE = Log;
INSERT INTO t_pr_merge_log SELECT number + 20000, number FROM numbers(100);
CREATE TABLE t_pr_merge_over_log ENGINE = Merge(currentDatabase(), '^t_pr_merge_(1|2|log)$');
SELECT trimLeft(explain) AS e FROM (EXPLAIN SELECT count(), sum(k), sum(v) FROM t_pr_merge_over_log) WHERE e IN ('Aggregating', 'MergingAggregated');
SELECT count(), sum(k), sum(v) FROM t_pr_merge_over_log;

-- A Merge over no tables at all must not be used either
SELECT '-- Merge over no tables';
CREATE TABLE t_pr_merge_over_nothing (k UInt64, v UInt64) ENGINE = Merge(currentDatabase(), '^t_pr_merge_no_such_tables');
SELECT trimLeft(explain) AS e FROM (EXPLAIN SELECT count(), sum(k), sum(v) FROM t_pr_merge_over_nothing) WHERE e IN ('Aggregating', 'MergingAggregated');
SELECT count(), sum(k), sum(v) FROM t_pr_merge_over_nothing;

DROP TABLE t_pr_merge_over_nothing;
DROP TABLE t_pr_merge_over_log;
DROP TABLE t_pr_merge_log;
DROP TABLE t_pr_merge;
DROP TABLE t_pr_merge_2;
DROP TABLE t_pr_merge_1;
