-- Tags: no-fasttest
-- no-fasttest: the dispatched arm needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

-- A task of a distributed query plan is a query of its own, so system.query_log must report the
-- task's own metrics in a row of its own, correlated to the initiator by initial_query_id. The
-- assertions below hold for both ways of running the tasks, on workers and in-process.

DROP TABLE IF EXISTS t_dp_task_metrics;

CREATE TABLE t_dp_task_metrics (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1024;

-- Merges stopped and one insert thread per insert, so the fixture leaves exactly two parts and the
-- number of parts the reader tasks select together is fixed.
SYSTEM STOP MERGES t_dp_task_metrics;
INSERT INTO t_dp_task_metrics SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
INSERT INTO t_dp_task_metrics SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;

SELECT throwIf(count() != 2, 'the fixture must leave exactly two active parts')
FROM system.parts
WHERE database = currentDatabase() AND table = 't_dp_task_metrics' AND active
FORMAT Null;

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

-- A dispatched task's row carries the worker's own default database, so the task rows of a query are
-- found through the initiator's id, never through current_database.
WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND log_comment = '05231_dispatched' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator
SELECT
    throwIf(count() = 0, 'dispatched execution logged no task row for the query'),
    throwIf(uniqExact(query_id) != count(), 'task rows do not carry a query_id of their own'),
    -- Three reader buckets, each selecting the fixture's two parts.
    throwIf(sum(ProfileEvents['SelectedParts']) != 6, 'the parts a task read are not reported in its own row'),
    throwIf(sum(ProfileEvents['DistributedPlanWorkerPartsReceived']) = 0,
            'the parts the coordinator assigned a task are not reported in its own row'),
    throwIf(sum(ProfileEvents['DistributedPlanWorkerPartsReceived'])
            != sum(ProfileEvents['DistributedPlanWorkerPartsScanned'])
             + sum(ProfileEvents['DistributedPlanWorkerPartsPruned']),
            'a task read or pruned a number of parts other than the number it was assigned')
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
  AND initial_query_id = initiator
-- The rows being counted are the measurement, so they are read locally rather than through a
-- distributed reader over a concurrently written system table.
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
FORMAT Null;

WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND log_comment = '05231_in_process' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator
SELECT
    throwIf(count() = 0, 'in-process execution logged no task row for the query'),
    throwIf(uniqExact(query_id) != count(), 'task rows do not carry a query_id of their own'),
    throwIf(sum(ProfileEvents['SelectedParts']) != 6, 'the parts a task read are not reported in its own row'),
    throwIf(sum(ProfileEvents['DistributedPlanWorkerPartsReceived']) = 0,
            'the parts the coordinator assigned a task are not reported in its own row'),
    throwIf(sum(ProfileEvents['DistributedPlanWorkerPartsReceived'])
            != sum(ProfileEvents['DistributedPlanWorkerPartsScanned'])
             + sum(ProfileEvents['DistributedPlanWorkerPartsPruned']),
            'a task read or pruned a number of parts other than the number it was assigned')
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
  AND initial_query_id = initiator
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
FORMAT Null;

DROP TABLE t_dp_task_metrics;
