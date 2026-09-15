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
DROP TABLE IF EXISTS t_probe_offset;

CREATE TABLE t_keys (k Int32) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_probe (k Int32, v UInt64) ENGINE = MergeTree ORDER BY k;

INSERT INTO t_keys SELECT number FROM numbers(10);
INSERT INTO t_probe SELECT number, number FROM numbers(100000);

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;
SET use_index_for_in_with_subqueries = 0;
-- A control's query condition cache entry can zero the next query's selection, and the setting is
-- randomized in CI, so pin it to keep each control comparable to the distributed query beside it.
SET use_query_condition_cache = 0;
-- Needed by the `EXPLAIN distributed = 1` assertions below: it is read while the plan is built, so a
-- SETTINGS clause on the explained query is too late.
SET distributed_plan_execute_locally = 1;

SELECT '-- empty shipped set, every part pruned locally';
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

-- Result equality alone cannot tell a restored read from one that never distributed: a read is left
-- serial when it selects no rows or stays under `distributed_plan_max_rows_to_broadcast`. `GatherExchange`
-- over the read is present only when the read itself was split into buckets. A bare SELECT is required:
-- with an aggregate the plan distributes on the aggregation alone.
SELECT '-- the plain read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0);

SELECT '-- non-empty shipped set, both sides agree';
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), sum(v) FROM t_probe WHERE k IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

-- The same divergence via a skip index rather than the primary key: `s` is not in the sorting key, so
-- only the minmax index over it can prune. This is the route the reported failure takes.
-- Only index-analysis-time pruning can diverge here: `make_distributed_plan` forces
-- `use_skip_indexes_on_data_read` off (`SettingsQuirks.cpp`), so a worker never applies skip indexes
-- while reading. See 04656_distributed_plan_workers_disable_jit_and_skip_index_read.
CREATE TABLE t_probe_skip (k Int32, s Int32, v UInt64, INDEX idx_s s TYPE minmax GRANULARITY 1)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_probe_skip SELECT number, number, number FROM numbers(100000);

SELECT '-- skip-index route, empty shipped set';
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- the skip-index read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0);

SELECT '-- skip-index route, non-empty shipped set';
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), sum(v) FROM t_probe_skip WHERE s IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- non-empty shipped set, _part_offset unaffected';
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe
WHERE k IN (SELECT k FROM t_keys WHERE k > 5);
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe
WHERE k IN (SELECT k FROM t_keys WHERE k > 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

-- `_part_offset` is read per part, so it must stay correct when whole parts come back from the restore.
-- Disjoint key ranges make the worker prune two of the three parts while the third still has a row.
-- Merges are stopped so the three parts cannot collapse into one and take the restore out of play.
CREATE TABLE t_probe_offset (k Int32, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_probe_offset;
INSERT INTO t_probe_offset SELECT number, number FROM numbers(1000);
INSERT INTO t_probe_offset SELECT 10000 + number, number FROM numbers(1000);
INSERT INTO t_probe_offset SELECT 20000 + number, number FROM numbers(1000);

SELECT '-- _part_offset over a restored part';
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe_offset
WHERE k IN (SELECT k FROM t_keys WHERE k = 5);
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe_offset
WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- the restored-part read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe_offset WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0);

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

SELECT '-- the across-lanes FINAL read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere_if_final = 1);

-- Local analysis prunes every part, so the coordinator's whole part list is restored and resolved
-- against it; no row matches the predicate, so zero rows is also what reading nothing would give.
SELECT '-- FINAL, every part pruned locally and restored, no row matches';
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k > 1000);
SELECT count(), sum(v) FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere_if_final = 1;

SELECT '-- the FINAL read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe_final FINAL WHERE k IN (SELECT k FROM t_keys WHERE k > 1000)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0,
    optimize_move_to_prewhere_if_final = 1);

DROP TABLE t_probe_final;
DROP TABLE t_probe_offset;
DROP TABLE t_probe_skip;
DROP TABLE t_probe;
DROP TABLE t_keys;
