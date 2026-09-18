-- Tags: no-fasttest
-- no-fasttest: the dispatched arm needs the stateless worker configuration (tests/config/config.d/distributed_query.xml).

-- A task of a distributed query plan is a query of its own, so system.query_log must report the
-- task's own metrics in a row of its own, correlated to the initiator by initial_query_id. The
-- assertions below hold for both ways of running the tasks, on workers and in-process, and for a
-- plan whose initiator is itself a secondary query. A table's max_concurrent_queries still counts
-- whole queries, so the last block asserts that the tasks of one plan do not reject one another.

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

-- A plan whose own initiator is a secondary query: the shard read below builds a plan of its own,
-- and its tasks belong to the query the client sent, not to the shard query. Reading a local
-- address in place leaves no nested initiator at all, so prefer_localhost_replica = 0 is what makes
-- the shard read a query of its own.
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_dp_task_metrics) WHERE k < 150000
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0, prefer_localhost_replica = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    use_query_condition_cache = 0, log_comment = '05231_nested'
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
) AS initiator,
(
    SELECT read_rows
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND log_comment = '05231_dispatched' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator_read_rows
SELECT
    -- A scalar subquery with no matching row yields the type default, and the row filter below would
    -- then degenerate into one that matches unrelated traffic.
    throwIf(initiator = '', 'dispatched execution: the initiator row was not found'),
    -- The initiator reads only what the exchanges hand it, one aggregate state per reader bucket, so
    -- its own row must stay far below the 150000 rows the tasks read (measured: 1).
    throwIf(initiator_read_rows > 1000, 'dispatched execution: the initiator row also reports the rows its tasks read'),
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
) AS initiator,
(
    SELECT read_rows
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND log_comment = '05231_in_process' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator_read_rows
SELECT
    throwIf(initiator = '', 'in-process execution: the initiator row was not found'),
    -- Same bound as above, and here it is what the tasks running in the initiator's own process must
    -- no longer add to its row (measured: 1 with the tasks attributed, 300185 without).
    throwIf(initiator_read_rows > 1000, 'in-process execution: the initiator row also reports the rows its tasks read'),
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

WITH (
    SELECT query_id
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase()
      AND log_comment = '05231_nested' AND is_initial_query AND type = 'QueryFinish'
    ORDER BY event_time_microseconds DESC
    LIMIT 1
) AS initiator
SELECT
    throwIf(initiator = '', 'nested execution: the initiator row was not found'),
    -- A task's query_id is `<plan uuid>::<task id>`, which is what tells a task row apart from the
    -- shard read's own secondary query. Both must be there: without the shard query no plan was
    -- nested, and the root the tasks carry would be trivially the initiator's own.
    throwIf(countIf(position(query_id, '::') = 0) = 0,
            'the shard read did not run as a query of its own, so no plan was nested'),
    throwIf(countIf(position(query_id, '::') > 0) = 0,
            'a dispatched task did not keep the outer query as its root')
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND is_initial_query = 0
  AND initial_query_id = initiator
SETTINGS enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0
FORMAT Null;

-- A table's max_concurrent_queries counts queries, so the reader buckets of one plan share its single
-- slot; a query that took one slot per bucket would reject itself against a limit of one.
CREATE TABLE t_dp_task_metrics_limited (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS index_granularity = 1024, max_concurrent_queries = 1, min_marks_to_honor_max_concurrent_queries = 1;

SYSTEM STOP MERGES t_dp_task_metrics_limited;
INSERT INTO t_dp_task_metrics_limited SELECT number, number FROM numbers(100000) SETTINGS max_insert_threads = 1;
INSERT INTO t_dp_task_metrics_limited SELECT number + 100000, number FROM numbers(100000) SETTINGS max_insert_threads = 1;

SELECT count() FROM t_dp_task_metrics_limited WHERE k < 150000
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    use_query_condition_cache = 0
FORMAT Null;

SELECT count() FROM t_dp_task_metrics_limited WHERE k < 150000
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 0,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, max_rows_to_group_by = 0,
    distributed_plan_max_rows_to_broadcast = 0, distributed_plan_default_reader_bucket_count = 3,
    use_query_condition_cache = 0
FORMAT Null;

DROP TABLE t_dp_task_metrics_limited;
DROP TABLE t_dp_task_metrics;
