-- A bucketed distributed read is pinned to the coordinator's part list, but the worker re-runs its own
-- index analysis. With `use_index_for_in_with_subqueries = 0` the coordinator cannot use the IN set while
-- the worker receives it as shipped tuple values and can, so the worker prunes a part the coordinator
-- selected. Such a part is dropped from the marks the worker reads, neither read nor reported missing:
-- each distributed query below must return what its single-node control returns.

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

-- Result equality alone cannot tell a bucketed read from one that never distributed: a read is left
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

-- `_part_offset` is read per part, so it must stay correct when whole parts drop out of the read.
-- Disjoint key ranges make the worker prune two of the three parts while the third still has a row.
-- Merges are stopped so the three parts cannot collapse into one and take the divergence out of play.
CREATE TABLE t_probe_offset (k Int32, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_probe_offset;
INSERT INTO t_probe_offset SELECT number, number FROM numbers(1000);
INSERT INTO t_probe_offset SELECT 10000 + number, number FROM numbers(1000);
INSERT INTO t_probe_offset SELECT 20000 + number, number FROM numbers(1000);

SELECT '-- _part_offset with whole parts pruned locally';
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe_offset
WHERE k IN (SELECT k FROM t_keys WHERE k = 5);
SELECT count(), min(_part_offset), max(_part_offset) FROM t_probe_offset
WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SELECT '-- the whole-parts-pruned read distributes', countIf(explain LIKE '%GatherExchange%') > 0
FROM (EXPLAIN distributed = 1 SELECT k, v FROM t_probe_offset WHERE k IN (SELECT k FROM t_keys WHERE k = 5)
SETTINGS make_distributed_plan = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0);

-- Results cannot show that a pruned part is not read: putting it back into the scan returns the same rows,
-- because the rows it holds do not match the predicate anyway. Read volume can. Only the first of the three
-- parts can match, so this reads its 1000 rows plus the key table; the coordinator cannot use the IN set, so
-- its marks cover all three parts and adding the pruned two back would read 3000.
SELECT '-- rows read by the probe below';
SELECT count() FROM t_probe_offset WHERE k IN (SELECT k FROM t_keys WHERE k = 5) -- read_volume_probe
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    enable_join_runtime_filters = 0, max_rows_to_group_by = 0;

SYSTEM FLUSH LOGS query_log;
-- Each fragment logs a query_log row of its own and the initiator's row reports only the exchange
-- input, so the volume is summed over the probe's task rows, correlated by `initial_query_id`. An
-- empty family sums to 0 and would pass the bound, so the family is asserted to exist. Measured 1020
-- rows here, against the 3020 the three-part read would take.
WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
        AND current_database = currentDatabase() AND type = 'QueryFinish' AND is_initial_query
        AND query LIKE '%read_volume_probe%' AND query NOT LIKE '%query_log%'
    ORDER BY event_time_microseconds DESC LIMIT 1
) AS probe
SELECT '-- the pruned parts are not read',
    probe != '' AND count() > 0 AND sum(read_rows) < 2000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time > now() - INTERVAL 10 MINUTE
    AND type = 'QueryFinish' AND is_initial_query = 0 AND initial_query_id = probe;

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

-- Local analysis prunes every part, so every lane loses all its marks and the read produces nothing.
-- Zero rows is also what an unpatched binary returns here, so this arm does not discriminate the two;
-- it is kept because nothing else covers a FINAL task whose lanes all end up empty.
SELECT '-- FINAL, every part pruned locally, no row matches';
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
