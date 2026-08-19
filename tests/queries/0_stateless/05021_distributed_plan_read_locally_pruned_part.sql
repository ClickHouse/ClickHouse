-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A bucketed distributed read is pinned to the coordinator's part list, but the worker re-runs its own
-- index analysis. With `use_index_for_in_with_subqueries = 0` the coordinator cannot use the IN set while
-- the worker receives it as shipped tuple values and can, so the worker prunes a part the coordinator
-- selected. Each distributed query below must return what its single-node control returns.

DROP TABLE IF EXISTS t_keys;
DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_probe_final;
DROP TABLE IF EXISTS t_probe_skip;

CREATE TABLE t_keys (k Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_probe (k Int32, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_keys SELECT number FROM numbers(10);
INSERT INTO t_probe SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET use_index_for_in_with_subqueries = 0;

SELECT '-- empty shipped set, every part pruned locally';
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- non-empty shipped set, both sides agree';
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

-- The same divergence via a skip index rather than the primary key: `s` is not in the sorting key, so
-- only the minmax index over it can prune. This is the route the reported failure takes.
CREATE TABLE t_probe_skip (k Int32, s Int32, v UInt64, INDEX idx_s s TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_skip SELECT number, number, number FROM numbers(100000);

SELECT '-- skip-index route, empty shipped set';
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- skip-index route, skip indexes applied while reading';
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0, use_skip_indexes_on_data_read = 1;

SELECT '-- skip-index route, non-empty shipped set';
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- _part_offset over a restored part';
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe
WHERE k IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe
WHERE k IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

-- FINAL resolves the coordinator's marks per lane, in a separate site from the plain read. Several parts
-- with disjoint primary-key ranges, spread over more lanes than there are buckets, make the local analysis
-- prune parts from lanes other than the first while the lane resolution still has a part to read.
CREATE TABLE t_probe_final (k Int32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY k;
SYSTEM STOP MERGES t_probe_final;
INSERT INTO t_probe_final SELECT number, number FROM numbers(1000);
INSERT INTO t_probe_final SELECT 10000 + number, number FROM numbers(1000);
INSERT INTO t_probe_final SELECT 20000 + number, number FROM numbers(1000);
INSERT INTO t_probe_final SELECT 30000 + number, number FROM numbers(1000);
INSERT INTO t_probe_final SELECT 40000 + number, number FROM numbers(1000);
INSERT INTO t_probe_final SELECT 50000 + number, number FROM numbers(1000);

SELECT '-- FINAL, parts pruned locally across lanes';
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k = 5);
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere_if_final = 1;

SELECT '-- FINAL, empty shipped set, every part pruned locally';
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere_if_final = 1;

DROP TABLE t_probe_final;
DROP TABLE t_probe_skip;
DROP TABLE t_probe;
DROP TABLE t_keys;
