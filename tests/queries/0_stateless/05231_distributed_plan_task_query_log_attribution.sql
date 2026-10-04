-- Tags: no-fasttest
-- no-fasttest: the dispatched arm needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

-- A task of a distributed query plan is a query of its own, so system.query_log must report the
-- task's own metrics in a row of its own, correlated to the initiator by initial_query_id. The
-- values recorded below hold for both ways of running the tasks, on workers and in-process: the
-- initiator's row reports none of the parts its tasks read, and each task row reports the parts the
-- coordinator assigned it, all of which it either scanned or pruned.

-- The stress profile sets `ast_fuzzer_runs = 5`; a fuzzed re-run inherits `log_comment` and would
-- win the lookup against `system.query_log` below.
SET ast_fuzzer_runs = 0;

DROP TABLE IF EXISTS t_dp_task_metrics;

CREATE TABLE t_dp_task_metrics (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;

-- Merges stopped and one insert thread per insert, so the fixture leaves exactly two parts and the
-- number of parts the reader tasks select together is fixed.
SYSTEM STOP MERGES t_dp_task_metrics;
INSERT INTO t_dp_task_metrics SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
INSERT INTO t_dp_task_metrics SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;

SELECT 'active parts', count()
FROM system.parts
WHERE database = currentDatabase() AND table = 't_dp_task_metrics' AND active;

-- The lookups below are bounded by the time this attempt started, so rows an earlier run left behind
-- cannot satisfy them when that run shared this database (`clickhouse-test --database`).
CREATE TEMPORARY TABLE start_ts AS (SELECT now() AS ts);

-- max_rows_to_group_by must be pinned to 0: the stateless profile sets it to 10G, which makes the
-- aggregation fall back to non-distributed execution and leaves no task at all.
SELECT count() FROM t_dp_task_metrics WHERE k < 150000
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    use_query_condition_cache = 0, log_comment = '05231_dispatched'
FORMAT Null;

SELECT count() FROM t_dp_task_metrics WHERE k < 150000
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    use_query_condition_cache = 0, log_comment = '05231_in_process'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

-- The initiator reads only what the exchanges hand it, one aggregate state per reader bucket, so its
-- own row reports neither the parts nor the rows its tasks read. Where the tasks are not rows of
-- their own, each of them logs one that claims to be the initial query and reports the whole query's
-- read_rows instead (measured: 300185 against 1).
SELECT 'dispatched initiator', count() > 0, max(ProfileEvents['SelectedParts']), max(read_rows) < 1000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
  AND current_database = currentDatabase()
  AND log_comment = '05231_dispatched' AND is_initial_query AND type = 'QueryFinish';

-- A dispatched task's row carries the worker's own default database, so the task rows of a query are
-- found through the initiator's id, never through current_database.
-- How a task's rows are divided between its own reads is not fixed, so the columns below are the
-- parts it was assigned: one row per reader bucket, each holding the two parts of the fixture.
WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
      AND current_database = currentDatabase()
      AND log_comment = '05231_dispatched' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator
SELECT 'dispatched tasks', uniqExact(query_id) = count(),
    countIf(ProfileEvents['DistributedPlanWorkerPartsReceived'] > 0),
    arraySort(groupArrayIf(ProfileEvents['SelectedParts'], ProfileEvents['DistributedPlanWorkerPartsReceived'] > 0)),
    sum(ProfileEvents['DistributedPlanWorkerPartsReceived']),
    sum(ProfileEvents['DistributedPlanWorkerPartsScanned']),
    sum(ProfileEvents['DistributedPlanWorkerPartsPruned'])
FROM system.query_log
-- An initiator that was not found leaves a filter matching unrelated traffic, so it is excluded here
-- and the empty counts below are what fail the test.
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
  AND type = 'QueryFinish' AND is_initial_query = 0
  AND initiator != '' AND initial_query_id = initiator
-- The rows being counted are the measurement, so they are read locally rather than through a
-- distributed reader over a concurrently written system table.
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

SELECT 'in-process initiator', count() > 0, max(ProfileEvents['SelectedParts']), max(read_rows) < 1000
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
  AND current_database = currentDatabase()
  AND log_comment = '05231_in_process' AND is_initial_query AND type = 'QueryFinish';

WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
      AND current_database = currentDatabase()
      AND log_comment = '05231_in_process' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator
SELECT 'in-process tasks', uniqExact(query_id) = count(),
    countIf(ProfileEvents['DistributedPlanWorkerPartsReceived'] > 0),
    arraySort(groupArrayIf(ProfileEvents['SelectedParts'], ProfileEvents['DistributedPlanWorkerPartsReceived'] > 0)),
    sum(ProfileEvents['DistributedPlanWorkerPartsReceived']),
    sum(ProfileEvents['DistributedPlanWorkerPartsScanned']),
    sum(ProfileEvents['DistributedPlanWorkerPartsPruned'])
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= (SELECT ts FROM start_ts)
  AND type = 'QueryFinish' AND is_initial_query = 0
  AND initiator != '' AND initial_query_id = initiator
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;

DROP TABLE t_dp_task_metrics;
