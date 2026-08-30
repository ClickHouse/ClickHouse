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

-- No `distributed_plan_execute_locally` here: only a dispatched fragment gets its own
-- process-list entry, and so its own counters. Either counter can carry the slot depending on
-- the allocator, so the assertion sums them. A zero on the initiator means slots went unmetered
-- server-wide, which no fragment can cause, so the assertion stands aside instead of guessing.
SELECT '-- dispatched worker fragments arbitrate CPU slots and honor the thread limit';
-- The stress profile sets `ast_fuzzer_runs = 5`; a fuzzed re-run inherits `log_comment` and would
-- win the lookup against `system.query_log` below.
SET ast_fuzzer_runs = 0;
SELECT g, count() FROM t_worker_count GROUP BY g ORDER BY g LIMIT 4
SETTINGS enable_cascades_optimizer = 1, make_distributed_plan = 1,
    enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0,
    distributed_plan_force_shuffle_aggregation = 1,
    use_concurrency_control = 1, max_threads = 1,
    log_comment = '04515_worker_fragment_cpu_slots'
FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT fragments > 0 AND (initiator_slots = 0 OR min_slots > 0),
       fragments > 0 AND (initiator_slots = 0 OR max_slots = 1)
FROM (
    SELECT maxIf(slots, is_initiator) AS initiator_slots,
           countIf(NOT is_initiator) AS fragments,
           minIf(slots, NOT is_initiator) AS min_slots,
           maxIf(slots, NOT is_initiator) AS max_slots
    FROM (
        SELECT query_id = initial_query_id AS is_initiator,
               ProfileEvents['ConcurrencyControlSlotsAcquired']
             + ProfileEvents['ConcurrencyControlSlotsAcquiredNonCompeting'] AS slots
        FROM system.query_log
        WHERE event_date >= yesterday() AND type = 'QueryFinish'
          AND initial_query_id = (
              SELECT query_id FROM system.query_log
              WHERE event_date >= yesterday() AND type = 'QueryFinish'
                AND current_database = currentDatabase()
                AND query_id = initial_query_id
                AND query_kind = 'Select'
                AND log_comment = '04515_worker_fragment_cpu_slots'
              ORDER BY event_time_microseconds DESC LIMIT 1)
    )
)
SETTINGS make_distributed_plan = 0;

DROP TABLE t_worker_count;
