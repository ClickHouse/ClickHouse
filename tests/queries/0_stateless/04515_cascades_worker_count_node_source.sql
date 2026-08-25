-- Tags: no-darwin, no-old-analyzer
-- no-darwin: distributed execution uses the streaming exchange, which is implemented only on Linux.
-- no-old-analyzer: distributed Cascades planning requires the analyzer, like the other make_distributed_plan tests.

-- `distributed_plan_workers_num` sets the node count Cascades plans for under
-- `distributed_plan_execute_locally`: one worker stays single-node, four distribute
-- (`distributed_plan_force_shuffle_aggregation` pins the shape so the check does not depend on a
-- cost tie). Without it the count comes from the configured worker cluster, so one worker would
-- still build a multi-node plan.

SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_worker_count;
CREATE TABLE t_worker_count (k UInt64, g UInt16) ENGINE = MergeTree ORDER BY k
    SETTINGS auto_statistics_types = '', index_granularity = 8192, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
-- Enough granules that a parallel read can split across nodes.
INSERT INTO t_worker_count SELECT number, number % 200 FROM numbers(1000000);

SELECT '-- one worker: local plan, no exchange';
EXPLAIN PLAN SELECT g, count() FROM t_worker_count GROUP BY g
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 1;

SELECT '-- four workers: distributed plan with shuffle and gather exchanges';
EXPLAIN PLAN SELECT g, count() FROM t_worker_count GROUP BY g
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 4;

-- A many-shard shuffle must still aggregate correctly and still distribute.
SELECT '-- sixteen workers: shuffled aggregation is correct';
SELECT g, count() FROM t_worker_count GROUP BY g ORDER BY g LIMIT 4
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 16;

SELECT '-- sixteen workers: still a distributed plan';
EXPLAIN PLAN SELECT g, count() FROM t_worker_count GROUP BY g
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1, distributed_plan_execute_locally = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0, distributed_plan_force_shuffle_aggregation = 1,
    distributed_plan_workers_num = 16;

-- Only a dispatched fragment reports its own counters: it gets its own process-list entry, while
-- a `distributed_plan_execute_locally` fragment reports the initiator's. A fragment carries the
-- initiator's `query_id` as its `initial_query_id`, which is unique per query, so that key selects
-- one query's fragments whatever else the server is running concurrently.
-- A fragment bounded to one thread acquires exactly one CPU slot, competing or not; which of the
-- two counters carries it depends on the server's scheduler, so the assertion sums them. That
-- value is vacuously true while arbitration is off, which is what the first one pins. The probe
-- runs with `make_distributed_plan = 0` so it spawns no fragments of its own.
SELECT '-- dispatched worker fragments arbitrate CPU slots and honor the thread limit';
-- The stress profile sets `ast_fuzzer_runs = 5`; a fuzzed re-run inherits `log_comment` and would
-- win the lookup against `system.query_log` below.
SET ast_fuzzer_runs = 0;
SET log_comment = '04515_worker_fragment_cpu_slots';
SELECT g, count() FROM t_worker_count GROUP BY g ORDER BY g LIMIT 4
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    distributed_plan_force_shuffle_aggregation = 1,
    use_concurrency_control = 1, max_threads = 1
FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT count() > 0 AND min(ProfileEvents['ConcurrencyControlSlotsGranted']) > 0,
       count() > 0 AND max(ProfileEvents['ConcurrencyControlSlotsAcquired']
                        + ProfileEvents['ConcurrencyControlSlotsAcquiredNonCompeting']) = 1
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish'
  AND initial_query_id = (
      SELECT query_id FROM system.query_log
      WHERE event_date >= yesterday() AND type = 'QueryFinish'
        AND current_database = currentDatabase()
        AND query_id = initial_query_id
        AND Settings['log_comment'] = '04515_worker_fragment_cpu_slots'
      ORDER BY event_time_microseconds DESC LIMIT 1)
  AND query_id != initial_query_id
SETTINGS make_distributed_plan = 0;
SET log_comment = '';

DROP TABLE t_worker_count;
